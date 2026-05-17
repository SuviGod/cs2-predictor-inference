package com.cs2predictor.inference.stream;

import com.cs2predictor.inference.schema.GsiSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class GsiEventParser {

    public static Dataset<Row> parse(Dataset<Row> raw) {
        return raw
                .select(
                        col("key").cast("string").alias("session_key"),
                        from_json(col("value").cast("string"), GsiSchema.get()).alias("gsi"),
                        col("timestamp").alias("event_timestamp")
                )
                // drop rows without a map (menu screens, loading, spectator-only events)
                .filter(col("gsi.map").isNotNull())
                .select(
                        col("session_key"),
                        col("gsi.map.round").alias("round_num"),
                        col("gsi.round.phase").alias("round_phase"),
                        col("gsi.phase_countdowns.phase_ends_in").alias("phase_ends_in"),
                        col("gsi.phase_countdowns.phase").alias("bomb_phase"),
                        // allplayers re-serialised as JSON string; Jackson re-parses it inside
                        // the stateful state function to avoid Kryo MapData serialisation bugs
                        to_json(col("gsi.allplayers")).alias("allplayers_json"),
                        col("event_timestamp")
                );
    }
}
