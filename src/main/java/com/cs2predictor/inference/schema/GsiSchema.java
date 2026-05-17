package com.cs2predictor.inference.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

public class GsiSchema {

    // Built once; shared by GsiEventParser (full schema) and
    // StatelessFeatureExtractor (allplayers re-parse after to_json round-trip)
    private static final StructType PLAYER_SCHEMA = buildPlayerSchema();

    private static StructType buildPlayerSchema() {
        StructType playerStateSchema = new StructType()
                .add("health",         DataTypes.IntegerType, true)
                .add("round_kills",    DataTypes.IntegerType, true)
                .add("round_totaldmg", DataTypes.IntegerType, true);

        StructType playerMatchStatsSchema = new StructType()
                .add("kills", DataTypes.IntegerType, true);

        return new StructType()
                .add("team",        DataTypes.StringType,      true)
                .add("state",       playerStateSchema,          true)
                .add("match_stats", playerMatchStatsSchema,     true);
    }

    public static MapType allplayersMapType() {
        return DataTypes.createMapType(DataTypes.StringType, PLAYER_SCHEMA);
    }

    public static StructType get() {
        StructType roundSchema = new StructType()
                .add("phase", DataTypes.StringType, true);

        StructType phaseCountdownsSchema = new StructType()
                .add("phase",         DataTypes.StringType, true)
                .add("phase_ends_in", DataTypes.StringType, true);  // float sent as string by GSI

        StructType mapSchema = new StructType()
                .add("round", DataTypes.IntegerType, true)
                .add("phase", DataTypes.StringType,  true);

        StructType providerSchema = new StructType()
                .add("steamid", DataTypes.StringType, true);

        return new StructType()
                .add("provider",         providerSchema,           true)
                .add("map",              mapSchema,                 true)
                .add("round",            roundSchema,               true)
                .add("phase_countdowns", phaseCountdownsSchema,     true)
                .add("allplayers",       allplayersMapType(),        true);
    }
}
