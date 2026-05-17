package com.cs2predictor.inference.config;

public class KafkaInputConfig {
    private String bootstrapServers;
    private String topic;
    private String startingOffsets;
    private String groupId;

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public String getStartingOffsets() { return startingOffsets; }
    public void setStartingOffsets(String startingOffsets) { this.startingOffsets = startingOffsets; }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }
}