package com.cs2predictor.inference.config;

public class SparkConfig {
    private String appName;
    private String master;
    private int shufflePartitions;
    private long triggerIntervalMs;

    public String getAppName() { return appName; }
    public void setAppName(String appName) { this.appName = appName; }

    public String getMaster() { return master; }
    public void setMaster(String master) { this.master = master; }

    public int getShufflePartitions() { return shufflePartitions; }
    public void setShufflePartitions(int shufflePartitions) { this.shufflePartitions = shufflePartitions; }

    public long getTriggerIntervalMs() { return triggerIntervalMs; }
    public void setTriggerIntervalMs(long triggerIntervalMs) { this.triggerIntervalMs = triggerIntervalMs; }
}