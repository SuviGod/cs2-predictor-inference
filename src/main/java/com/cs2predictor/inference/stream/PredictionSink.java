package com.cs2predictor.inference.stream;

import com.cs2predictor.inference.config.InferenceConfig;
import com.cs2predictor.inference.config.KafkaOutputConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import static org.apache.spark.sql.functions.*;

public class PredictionSink {

    public static StreamingQuery write(Dataset<Row> predictions, InferenceConfig config)
            throws Exception {

        String mode = config.getOutput().getMode();
        long triggerMs = config.getSpark().getTriggerIntervalMs();

        if ("console".equals(mode)) {
            return predictions
                    .writeStream()
                    .format("console")
                    .option("truncate", "false")
                    .option("checkpointLocation", "checkpoint/predictions-console")
                    .outputMode("append")
                    .trigger(Trigger.ProcessingTime(triggerMs))
                    .start();
        }

        // Kafka mode — build JSON value with camelCase field names matching the design contract:
        // { "sessionKey": "...", "round": N, "probCtWin": 0.73, "timestamp": <epoch ms> }
        Dataset<Row> kafkaReady = predictions.select(
                col("session_key").alias("key"),
                to_json(struct(
                        col("session_key").alias("sessionKey"),
                        col("round_num").alias("round"),
                        col("prob_ct_win").alias("probCtWin"),
                        col("event_timestamp").alias("timestamp")
                )).alias("value")
        );

        KafkaOutputConfig kafkaOut = config.getKafkaOutput();
        return kafkaReady
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaOut.getBootstrapServers())
                .option("topic", kafkaOut.getTopic())
                .option("checkpointLocation", "checkpoint/predictions-kafka")
                .outputMode("append")
                .trigger(Trigger.ProcessingTime(triggerMs))
                .start();
    }
}
