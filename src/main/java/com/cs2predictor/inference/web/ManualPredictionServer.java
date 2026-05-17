package com.cs2predictor.inference.web;

import com.cs2predictor.inference.config.WebConfig;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.apache.spark.ml.functions.vector_to_array;
import static org.apache.spark.sql.functions.col;

public class ManualPredictionServer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Column names must exactly match the VectorAssembler's inputCols in the trained PipelineModel.
    // Column order in the schema is irrelevant — VectorAssembler selects by name.
    private static final StructType FEATURE_SCHEMA = new StructType()
            .add("ct_alive",                  DataTypes.IntegerType)
            .add("t_alive",                   DataTypes.IntegerType)
            .add("ct_total_hp",               DataTypes.IntegerType)
            .add("t_total_hp",                DataTypes.IntegerType)
            .add("bomb_planted",              DataTypes.IntegerType)
            .add("remaining_time",            DataTypes.FloatType)
            .add("ct_alive_kills_prev3_sum",  DataTypes.IntegerType)
            .add("ct_alive_damage_prev3_sum", DataTypes.IntegerType)
            .add("t_alive_kills_prev3_sum",   DataTypes.IntegerType)
            .add("t_alive_damage_prev3_sum",  DataTypes.IntegerType);

    public static void start(WebConfig config, PipelineModel model, SparkSession spark) {
        try {
            HttpServer server = HttpServer.create(
                    new InetSocketAddress(config.getPort()), 0);
            server.createContext("/api/predict",
                    exchange -> handle(exchange, model, spark));
            // Two threads: one for the request, one spare for concurrent preflight.
            server.setExecutor(Executors.newFixedThreadPool(2));
            server.start();
            System.out.println("[ManualPredictionServer] Listening on port " + config.getPort());
        } catch (IOException e) {
            throw new RuntimeException("Failed to start ManualPredictionServer", e);
        }
    }

    private static void handle(HttpExchange exchange, PipelineModel model, SparkSession spark)
            throws IOException {

        exchange.getResponseHeaders().add("Access-Control-Allow-Origin",  "*");
        exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "POST, OPTIONS");
        exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");

        if ("OPTIONS".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }
        if (!"POST".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        try {
            PredictRequest req = MAPPER.readValue(
                    exchange.getRequestBody(), PredictRequest.class);

            // Row values must follow the same column order as FEATURE_SCHEMA above.
            Row row = RowFactory.create(
                    req.ctAlive,
                    req.tAlive,
                    req.ctTotalHp,
                    req.tTotalHp,
                    req.bombPlanted,
                    req.remainingTime,
                    req.ctAliveKillsPrev3,
                    req.ctAliveDamagePrev3,
                    req.tAliveKillsPrev3,
                    req.tAliveDamagePrev3
            );

            // newSession() shares the underlying SparkContext with the streaming query
            // but has isolated SQL configuration — safe for concurrent batch requests.
            SparkSession session = spark.newSession();
            Dataset<Row> df = session.createDataFrame(
                    Collections.singletonList(row), FEATURE_SCHEMA);

            double probCtWin = model.transform(df)
                    .select(vector_to_array(col("probability"), "float64").getItem(1))
                    .first()
                    .getDouble(0);

            writeJson(exchange, 200, Map.of("probCtWin", probCtWin));

        } catch (Exception e) {
            writeJson(exchange, 500, Map.of("error",
                    e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }
    }

    private static void writeJson(HttpExchange exchange, int status, Object body)
            throws IOException {
        byte[] bytes = MAPPER.writeValueAsBytes(body);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class PredictRequest {
        public int   ctAlive;
        public int   tAlive;
        public int   ctTotalHp;
        public int   tTotalHp;
        public int   bombPlanted;
        public float remainingTime;
        public int   ctAliveKillsPrev3;
        public int   ctAliveDamagePrev3;
        public int   tAliveKillsPrev3;
        public int   tAliveDamagePrev3;
    }
}
