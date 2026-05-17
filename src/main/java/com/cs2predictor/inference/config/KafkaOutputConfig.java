package com.cs2predictor.inference.config;

public class KafkaOutputConfig {
    private String bootstrapServers;
    private String topic;

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
}