package com.cs2predictor.inference.model;

import com.cs2predictor.inference.config.ModelConfig;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.SparkSession;

public class ModelLoader {

    public static PipelineModel load(ModelConfig config, SparkSession spark) {
        return PipelineModel.load(config.getPipelineModelPath());
    }
}
