package com.cs2predictor.inference.spark;

import com.cs2predictor.inference.config.SparkConfig;
import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

    public static SparkSession create(SparkConfig config) {
        return SparkSession.builder()
                .appName(config.getAppName())
                .master(config.getMaster())
                .config("spark.sql.shuffle.partitions", config.getShufflePartitions())
                .getOrCreate();
    }
}
