package com.cs2predictor.inference.stream;

import com.cs2predictor.inference.config.KafkaInputConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GsiStreamReader {

    public static Dataset<Row> read(SparkSession spark, KafkaInputConfig config) {
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.getBootstrapServers())
                .option("subscribe", config.getTopic())
                .option("startingOffsets", config.getStartingOffsets())
                .option("kafka.group.id", config.getGroupId())
                .load();
    }
}
