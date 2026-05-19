package com.cs2predictor.inference.stream;

import com.cs2predictor.inference.config.KafkaInputConfig;
import com.cs2predictor.inference.config.KafkaSecurityConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;

public class GsiStreamReader {

    public static Dataset<Row> read(SparkSession spark, KafkaInputConfig config, KafkaSecurityConfig security) {
        DataStreamReader reader = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.getBootstrapServers())
                .option("subscribe", config.getTopic())
                .option("startingOffsets", config.getStartingOffsets())
                .option("kafka.group.id", config.getGroupId())
                .option("failOnDataLoss", "false");

        if (security.isSaslEnabled()) {
            reader = reader
                    .option("kafka.security.protocol",  security.getSecurityProtocol())
                    .option("kafka.sasl.mechanism",      security.getSaslMechanism())
                    .option("kafka.sasl.jaas.config",    security.buildJaasConfig());
        }

        return reader.load();
    }
}