package com.cs2predictor.inference.schema;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

class GsiSchemaTest {

    private static SparkSession spark;

    // Minimal representative GSI payload covering all schema fields used by inference
    private static final String SAMPLE_GSI = """
            {
              "provider": { "steamid": "76561198329629332" },
              "map":      { "round": 2, "phase": "live" },
              "round":    { "phase": "live" },
              "phase_countdowns": { "phase": "live", "phase_ends_in": "91.6" },
              "allplayers": {
                "76561198329629332": {
                  "team": "CT",
                  "state":       { "health": 100, "round_kills": 0, "round_totaldmg": 0 },
                  "match_stats": { "kills": 3 }
                },
                "76561198329629333": {
                  "team": "T",
                  "state":       { "health": 75, "round_kills": 1, "round_totaldmg": 80 },
                  "match_stats": { "kills": 1 }
                }
              }
            }
            """;

    @BeforeAll
    static void setup() {
        spark = SparkSession.builder()
                .master("local")
                .appName("GsiSchemaTest")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void teardown() {
        if (spark != null) spark.stop();
    }

    @Test
    void parseGsiExample_keyFieldsExtracted() {
        Dataset<Row> input = spark.createDataFrame(
                List.of(RowFactory.create(SAMPLE_GSI)),
                new StructType().add("value", DataTypes.StringType)
        );

        Dataset<Row> result = input.select(
                from_json(col("value"), GsiSchema.get()).alias("gsi")
        ).select(
                col("gsi.map.round").alias("round_num"),
                col("gsi.round.phase").alias("round_phase"),
                col("gsi.phase_countdowns.phase_ends_in").alias("phase_ends_in"),
                col("gsi.map").alias("map_struct"),
                to_json(col("gsi.allplayers")).alias("allplayers_json")
        );

        Row row = result.first();

        assertEquals(2,      row.<Integer>getAs("round_num"),    "round_num should be 2");
        assertEquals("live", row.<String>getAs("round_phase"),   "round_phase should be live");
        assertEquals("91.6", row.<String>getAs("phase_ends_in"), "phase_ends_in should be 91.6");
        assertNotNull(row.getAs("map_struct"),                    "map struct must not be null");

        String allplayersJson = row.getAs("allplayers_json");
        assertNotNull(allplayersJson, "allplayers_json must not be null");
        assertTrue(allplayersJson.contains("76561198329629332"), "CT player steamid must be present");
        assertTrue(allplayersJson.contains("76561198329629333"), "T player steamid must be present");
    }

    @Test
    void missingMapField_parsedAsNull() {
        String noMapJson = """
                { "round": { "phase": "live" }, "provider": { "steamid": "123" } }
                """;

        Dataset<Row> input = spark.createDataFrame(
                List.of(RowFactory.create(noMapJson)),
                new StructType().add("value", DataTypes.StringType)
        );

        Row row = input.select(
                from_json(col("value"), GsiSchema.get()).alias("gsi")
        ).select(col("gsi.map").alias("map_struct")).first();

        assertNull(row.getAs("map_struct"), "absent map field must parse as null (filter target)");
    }
}
