package com.cs2predictor.inference.util;

// Aggregation of alive-player stats is done inline in StatelessFeatureExtractor
// via Spark SQL higher-order functions (map_filter / aggregate), which avoids
// a groupBy shuffle and keeps the pipeline in Append output mode.
// This class is reserved for any future per-player helper logic.
public class AlivePlayerAggregator {
}
