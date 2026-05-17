package com.cs2predictor.inference.stream;

import com.cs2predictor.inference.schema.GsiSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class StatelessFeatureExtractor {

    // Cannot use groupBy().agg() here: that requires Complete/Update output mode,
    // which is incompatible with the downstream flatMapGroupsWithState (Append only).
    // All per-event aggregation is done within a single row using SQL higher-order
    // functions (map_filter, aggregate) so no shuffle or output-mode change is needed.

    public static Dataset<Row> extract(Dataset<Row> parsed) {
        // Re-parse allplayers_json (produced by to_json in GsiEventParser) back into
        // a Spark map so we can apply map_filter / aggregate on it within this row.
        return parsed
                .withColumn("allplayers", from_json(col("allplayers_json"), GsiSchema.allplayersMapType()))
                .select(
                        col("session_key"),
                        col("round_num"),
                        col("round_phase"),
                        col("allplayers_json"),   // forwarded to state function
                        col("event_timestamp"),

                        // Alive player counts (health > 0)
                        expr("coalesce(size(map_filter(allplayers, (k, v) -> v.team = 'CT' AND v.state.health > 0)), 0)")
                                .alias("ct_alive"),
                        expr("coalesce(size(map_filter(allplayers, (k, v) -> v.team = 'T'  AND v.state.health > 0)), 0)")
                                .alias("t_alive"),

                        // Total HP for alive players per side
                        expr("coalesce(aggregate(" +
                                "  map_values(map_filter(allplayers, (k, v) -> v.team = 'CT' AND v.state.health > 0))," +
                                "  0," +
                                "  (acc, v) -> acc + v.state.health" +
                                "), 0)").alias("ct_total_hp"),
                        expr("coalesce(aggregate(" +
                                "  map_values(map_filter(allplayers, (k, v) -> v.team = 'T'  AND v.state.health > 0))," +
                                "  0," +
                                "  (acc, v) -> acc + v.state.health" +
                                "), 0)").alias("t_total_hp"),

                        // bomb_planted: 1 when round.phase == "bomb" (post-plant)
                        expr("CASE WHEN round_phase = 'bomb' THEN 1 ELSE 0 END")
                                .alias("bomb_planted"),

                        // remaining_time: pre-plant = round timer, post-plant = bomb fuse.
                        // coalesce to 0.0 guards against null during non-live phases.
                        coalesce(col("phase_ends_in").cast(DataTypes.FloatType), lit(0.0f))
                                .alias("remaining_time")
                );
    }
}
