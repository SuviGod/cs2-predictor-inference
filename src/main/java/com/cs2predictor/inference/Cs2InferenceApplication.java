package com.cs2predictor.inference;

import com.cs2predictor.inference.config.ConfigLoader;
import com.cs2predictor.inference.config.InferenceConfig;
import com.cs2predictor.inference.model.ModelLoader;
import com.cs2predictor.inference.spark.SparkSessionFactory;
import com.cs2predictor.inference.stream.GsiEventParser;
import com.cs2predictor.inference.stream.GsiStreamReader;
import com.cs2predictor.inference.stream.PredictionSink;
import com.cs2predictor.inference.stream.SessionStateFunction;
import com.cs2predictor.inference.stream.StatelessFeatureExtractor;
import com.cs2predictor.inference.web.ManualRequestConsumer;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static org.apache.spark.ml.functions.vector_to_array;
import static org.apache.spark.sql.functions.*;

public class Cs2InferenceApplication {

    private static final Logger log = LoggerFactory.getLogger(Cs2InferenceApplication.class);

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : "config/inference-config.yaml";
        log.info("Loading config from {}", configPath);
        InferenceConfig config = ConfigLoader.load(configPath);

        clearCheckpoints();

        SparkSession spark = SparkSessionFactory.create(config.getSpark());
        log.info("SparkSession created (master={})", config.getSpark().getMaster());

        log.info("Loading PipelineModel from {}", config.getModel().getPipelineModelPath());
        PipelineModel model = ModelLoader.load(config.getModel(), spark);
        log.info("PipelineModel loaded");

        // Manual prediction via Kafka — daemon thread so it shuts down with the JVM
        Thread manualThread = new Thread(
                () -> ManualRequestConsumer.start(
                        config.getKafkaInput().getBootstrapServers(),
                        config.getKafkaSecurity(),
                        model, spark),
                "manual-prediction-consumer"
        );
        manualThread.setDaemon(true);
        manualThread.start();

        // Step 1: raw Kafka stream — key=steamId, value=GSI JSON string
        Dataset<Row> raw = GsiStreamReader.read(spark, config.getKafkaInput(), config.getKafkaSecurity());

        // Step 2: parse GSI JSON → structured columns; filter non-game events
        Dataset<Row> parsed = GsiEventParser.parse(raw);

        // Step 3: stateless features (ct_alive, t_alive, hp, bomb_planted, remaining_time)
        Dataset<Row> withStateless = StatelessFeatureExtractor.extract(parsed);

        // Step 4: stateful prev3 features via flatMapGroupsWithState
        Dataset<Row> features = SessionStateFunction.apply(withStateless);

        // Step 5: apply PipelineModel; extract CT win probability from the probability vector.
        // probability[0] = P(label=0, T wins), probability[1] = P(label=1, CT wins).
        // ML-internal columns (features, scaled_features, prediction) are dropped here so they
        // are never serialised into the Kafka value or checkpoint.
        Dataset<Row> predictions = model.transform(features)
                .withColumn("prob_ct_win", vector_to_array(col("probability"), "float64").getItem(1))
                .select(
                        col("session_key"),
                        col("round_num"),
                        col("event_timestamp"),
                        col("prob_ct_win")
                );

        // Step 6: write predictions to Kafka topic (or console for development)
        StreamingQuery query = PredictionSink.write(predictions, config);
        log.info("Streaming query started (output.mode={})", config.getOutput().getMode());
        query.awaitTermination();
    }

    private static void clearCheckpoints() throws IOException {
        for (String dir : new String[]{"checkpoint/predictions-console", "checkpoint/predictions-kafka"}) {
            Path path = Paths.get(dir);
            if (Files.exists(path)) {
                Files.walkFileTree(path, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }
                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
                log.info("Cleared checkpoint: {}", dir);
            }
        }
    }
}
