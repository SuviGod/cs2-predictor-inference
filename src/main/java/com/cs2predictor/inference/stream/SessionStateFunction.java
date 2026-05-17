package com.cs2predictor.inference.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class SessionStateFunction {

    // RowEncoder.apply() was removed in Spark 4.x and Encoders.row(schema) is not yet public.
    // Additionally, Spark 4.x's Row encoder uses KryoSerializationCodec which is NOT
    // Java-serializable, causing "Task not serializable" when FlatMapGroupsWithStateExec tries
    // to ship tasks. Fix: convert each input row to a JSON string before groupByKey so the
    // value encoder is Encoders.STRING() throughout. event_timestamp is cast to Long (epoch-ms)
    // first so it survives the JSON round-trip without date-format ambiguity.

    static final ObjectMapper MAPPER = new ObjectMapper();

    // event_timestamp stored as epoch-milliseconds (Long) so it survives a JSON round-trip
    // without custom timestamp parsing.
    static final StructType OUTPUT_SCHEMA = new StructType()
            .add("session_key",               DataTypes.StringType,  true)
            .add("round_num",                 DataTypes.IntegerType, true)
            .add("event_timestamp",           DataTypes.LongType,    true)
            .add("ct_alive",                  DataTypes.IntegerType, true)
            .add("t_alive",                   DataTypes.IntegerType, true)
            .add("ct_total_hp",               DataTypes.IntegerType, true)
            .add("t_total_hp",                DataTypes.IntegerType, true)
            .add("bomb_planted",              DataTypes.IntegerType, true)
            .add("remaining_time",            DataTypes.FloatType,   true)
            .add("ct_alive_kills_prev3_sum",  DataTypes.IntegerType, true)
            .add("ct_alive_damage_prev3_sum", DataTypes.IntegerType, true)
            .add("t_alive_kills_prev3_sum",   DataTypes.IntegerType, true)
            .add("t_alive_damage_prev3_sum",  DataTypes.IntegerType, true);

    public static Dataset<Row> apply(Dataset<Row> withStateless) {
        // Serialise each input row to a JSON string so the value encoder in the downstream
        // groupByKey / flatMapGroupsWithState chain is Encoders.STRING() — a plain Java
        // ExpressionEncoder with no Kryo dependency.
        Dataset<String> inputJson = withStateless
                .withColumn("event_timestamp", col("event_timestamp").cast(DataTypes.LongType))
                .select(to_json(struct("*")).alias("_v"))
                .as(Encoders.STRING());

        Dataset<String> jsonRows = inputJson
                .groupByKey(
                        (MapFunction<String, String>) jsonStr ->
                                MAPPER.readTree(jsonStr).path("session_key").asText(""),
                        Encoders.STRING()
                )
                .flatMapGroupsWithState(
                        new StateFunction(),
                        OutputMode.Append(),
                        Encoders.javaSerialization(MatchSessionState.class),
                        Encoders.STRING(),
                        GroupStateTimeout.ProcessingTimeTimeout()
                );

        // Wrap each output JSON string in a single-column frame, then expand struct fields.
        return jsonRows
                .toDF("_json")
                .select(from_json(col("_json"), OUTPUT_SCHEMA).alias("f"))
                .select("f.*");
    }

    // =========================================================================
    // FlatMapGroupsWithStateFunction — receives JSON strings, emits JSON strings
    // =========================================================================

    static class StateFunction
            implements FlatMapGroupsWithStateFunction<String, String, MatchSessionState, String> {

        @Override
        public Iterator<String> call(
                String sessionKey,
                Iterator<String> values,
                GroupState<MatchSessionState> state) throws Exception {

            if (state.hasTimedOut()) {
                state.remove();
                return Collections.emptyIterator();
            }

            MatchSessionState matchState = state.exists() ? state.get() : new MatchSessionState();

            // Parse input JSON strings and sort by event_timestamp for correct phase ordering.
            List<JsonNode> inputs = new ArrayList<>();
            while (values.hasNext()) {
                inputs.add(MAPPER.readTree(values.next()));
            }
            inputs.sort(Comparator.comparingLong(n -> n.path("event_timestamp").asLong(0L)));

            state.setTimeoutDuration("4 hours");

            List<String> output = new ArrayList<>();

            for (JsonNode input : inputs) {
                String phase  = input.path("round_phase").asText("");
                int roundNum  = input.path("round_num").asInt(0);
                String apJson = input.path("allplayers_json").asText("{}");

                if ("over".equals(phase)) {
                    if (!matchState.roundArchived && roundNum == matchState.currentRound) {
                        archiveRound(matchState, apJson);
                        matchState.roundArchived = true;
                    }
                } else if ("freezetime".equals(phase)) {
                    if (roundNum > matchState.currentRound) {
                        saveRoundStart(matchState, apJson);
                        matchState.currentRound = roundNum;
                        matchState.roundArchived = false;
                    }
                } else if ("live".equals(phase) || "bomb".equals(phase)) {
                    int[] p = computePrev3(matchState, apJson);
                    output.add(buildOutputJson(input, p));
                }
            }

            state.update(matchState);
            return output.iterator();
        }
    }

    // Serialise one output row to JSON for the from_json round-trip in apply().
    private static String buildOutputJson(JsonNode input, int[] p) throws Exception {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("session_key",               input.path("session_key").asText(null));
        m.put("round_num",                 input.path("round_num").asInt(0));
        m.put("event_timestamp",           input.path("event_timestamp").asLong(0L));
        m.put("ct_alive",                  input.path("ct_alive").asInt(0));
        m.put("t_alive",                   input.path("t_alive").asInt(0));
        m.put("ct_total_hp",               input.path("ct_total_hp").asInt(0));
        m.put("t_total_hp",                input.path("t_total_hp").asInt(0));
        m.put("bomb_planted",              input.path("bomb_planted").asInt(0));
        m.put("remaining_time",            (float) input.path("remaining_time").asDouble(0.0));
        m.put("ct_alive_kills_prev3_sum",  p[0]);
        m.put("ct_alive_damage_prev3_sum", p[1]);
        m.put("t_alive_kills_prev3_sum",   p[2]);
        m.put("t_alive_damage_prev3_sum",  p[3]);
        return MAPPER.writeValueAsString(m);
    }

    // =========================================================================
    // State helpers — package-private static for unit-testability without Spark
    // =========================================================================

    /**
     * Archive end-of-round stats into per-player history (deque capped at 3).
     * Must be called during "over" while round_totaldmg is still valid.
     * Per-round kills = cumulative match_stats.kills − kills saved at round start.
     */
    static void archiveRound(MatchSessionState state, String allplayersJson) throws Exception {
        JsonNode root = MAPPER.readTree(allplayersJson);
        Iterator<Map.Entry<String, JsonNode>> it = root.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            String steamId = entry.getKey();
            JsonNode player = entry.getValue();
            int damage    = player.path("state").path("round_totaldmg").asInt(0);
            int cumKills  = player.path("match_stats").path("kills").asInt(0);
            int startKills = state.killsAtRoundStart.getOrDefault(steamId, cumKills);
            int roundKills = Math.max(0, cumKills - startKills);
            Deque<RoundStats> hist =
                    state.playerHistory.computeIfAbsent(steamId, k -> new ArrayDeque<>());
            hist.addLast(new RoundStats(roundKills, damage));
            if (hist.size() > 3) hist.removeFirst();
        }
    }

    /**
     * Snapshot each player's cumulative kill count at the start of a new round.
     * Called during "freezetime" when round_num advances.
     */
    static void saveRoundStart(MatchSessionState state, String allplayersJson) throws Exception {
        JsonNode root = MAPPER.readTree(allplayersJson);
        Iterator<Map.Entry<String, JsonNode>> it = root.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            state.killsAtRoundStart.put(
                    entry.getKey(),
                    entry.getValue().path("match_stats").path("kills").asInt(0)
            );
        }
    }

    /**
     * Sum prev-3-round kills and damage for currently-alive players per side.
     * Returns [ctKills, ctDmg, tKills, tDmg].
     * Players with no history (joined mid-match) contribute 0.
     */
    static int[] computePrev3(MatchSessionState state, String allplayersJson) throws Exception {
        int ctKills = 0, ctDmg = 0, tKills = 0, tDmg = 0;
        JsonNode root = MAPPER.readTree(allplayersJson);
        Iterator<Map.Entry<String, JsonNode>> it = root.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            JsonNode player = entry.getValue();
            if (player.path("state").path("health").asInt(0) <= 0) continue;
            Deque<RoundStats> hist = state.playerHistory.get(entry.getKey());
            if (hist == null) continue;
            String team = player.path("team").asText("");
            int kills = hist.stream().mapToInt(RoundStats::kills).sum();
            int dmg   = hist.stream().mapToInt(RoundStats::damage).sum();
            if ("CT".equals(team)) { ctKills += kills; ctDmg += dmg; }
            else if ("T".equals(team)) { tKills += kills; tDmg += dmg; }
        }
        return new int[]{ctKills, ctDmg, tKills, tDmg};
    }

    // =========================================================================
    // State schema (serialised with Java serialization)
    // =========================================================================

    public static class MatchSessionState implements Serializable {
        public int currentRound = 0;
        public boolean roundArchived = false;
        public Map<String, Integer>           killsAtRoundStart = new HashMap<>();
        public Map<String, Deque<RoundStats>> playerHistory     = new HashMap<>();
    }

    public record RoundStats(int kills, int damage) implements Serializable {}
}
