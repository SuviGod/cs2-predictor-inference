package com.cs2predictor.inference.config;

public class InferenceConfig {
    private SparkConfig spark;
    private KafkaInputConfig kafkaInput;
    private KafkaOutputConfig kafkaOutput;
    private KafkaSecurityConfig kafkaSecurity = new KafkaSecurityConfig();
    private ModelConfig model;
    private OutputConfig output;

    public SparkConfig getSpark() { return spark; }
    public void setSpark(SparkConfig spark) { this.spark = spark; }

    public KafkaInputConfig getKafkaInput() { return kafkaInput; }
    public void setKafkaInput(KafkaInputConfig kafkaInput) { this.kafkaInput = kafkaInput; }

    public KafkaOutputConfig getKafkaOutput() { return kafkaOutput; }
    public void setKafkaOutput(KafkaOutputConfig kafkaOutput) { this.kafkaOutput = kafkaOutput; }

    public KafkaSecurityConfig getKafkaSecurity() { return kafkaSecurity; }
    public void setKafkaSecurity(KafkaSecurityConfig kafkaSecurity) { this.kafkaSecurity = kafkaSecurity; }

    public ModelConfig getModel() { return model; }
    public void setModel(ModelConfig model) { this.model = model; }

    public OutputConfig getOutput() { return output; }
    public void setOutput(OutputConfig output) { this.output = output; }
}