package com.cs2predictor.inference.web;

import com.cs2predictor.inference.config.KafkaSecurityConfig;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.apache.spark.ml.functions.vector_to_array;
import static org.apache.spark.sql.functions.col;

public class ManualRequestConsumer {

    private static final Logger log = LoggerFactory.getLogger(ManualRequestConsumer.class);

    private static final String REQUEST_TOPIC  = "cs2.gsi.manual.requests";
    private static final String RESPONSE_TOPIC = "cs2.gsi.manual.responses";
    private static final ObjectMapper MAPPER = new ObjectMapper();

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

    public static void start(String bootstrapServers, KafkaSecurityConfig security,
                             PipelineModel model, SparkSession spark) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "cs2-manual-predictor");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        if (security.isSaslEnabled()) {
            for (Properties props : new Properties[]{consumerProps, producerProps}) {
                props.put("security.protocol",         security.getSecurityProtocol());
                props.put(SaslConfigs.SASL_MECHANISM,  security.getSaslMechanism());
                props.put(SaslConfigs.SASL_JAAS_CONFIG, security.buildJaasConfig());
            }
        }

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(Collections.singletonList(REQUEST_TOPIC));
            log.info("Listening on topic: {}", REQUEST_TOPIC);

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (var record : records) {
                    processRequest(record.value(), producer, model, spark);
                }
            }
        } catch (Exception e) {
            if (!Thread.currentThread().isInterrupted()) {
                log.error("Fatal error", e);
            }
        }
    }

    private static void processRequest(String json, KafkaProducer<String, String> producer,
                                       PipelineModel model, SparkSession spark) {
        String correlationId = null;
        try {
            PredictRequest req = MAPPER.readValue(json, PredictRequest.class);
            correlationId = req.correlationId;

            Row row = RowFactory.create(
                    req.ctAlive, req.tAlive, req.ctTotalHp, req.tTotalHp,
                    req.bombPlanted, req.remainingTime,
                    req.ctAliveKillsPrev3, req.ctAliveDamagePrev3,
                    req.tAliveKillsPrev3, req.tAliveDamagePrev3
            );

            // newSession() shares the underlying SparkContext — safe for concurrent batch transforms.
            SparkSession session = spark.newSession();
            Dataset<Row> df = session.createDataFrame(Collections.singletonList(row), FEATURE_SCHEMA);

            double probCtWin = model.transform(df)
                    .select(vector_to_array(col("probability"), "float64").getItem(1))
                    .first()
                    .getDouble(0);

            String response = MAPPER.writeValueAsString(Map.of(
                    "correlationId", correlationId,
                    "probCtWin", probCtWin
            ));
            producer.send(new ProducerRecord<>(RESPONSE_TOPIC, correlationId, response));

        } catch (Exception e) {
            if (correlationId != null) {
                try {
                    String errResponse = MAPPER.writeValueAsString(Map.of(
                            "correlationId", correlationId,
                            "error", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()
                    ));
                    producer.send(new ProducerRecord<>(RESPONSE_TOPIC, correlationId, errResponse));
                } catch (Exception ignored) {}
            }
            log.error("Failed to process request correlationId={}", correlationId, e);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class PredictRequest {
        public String correlationId;
        public int    ctAlive;
        public int    tAlive;
        public int    ctTotalHp;
        public int    tTotalHp;
        public int    bombPlanted;
        public float  remainingTime;
        public int    ctAliveKillsPrev3;
        public int    ctAliveDamagePrev3;
        public int    tAliveKillsPrev3;
        public int    tAliveDamagePrev3;
    }
}
