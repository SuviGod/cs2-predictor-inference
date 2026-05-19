# CS2 Round-Outcome Predictor — Inference Application Design

> **Purpose of this file:** complete design for the `cs2-predictor-inference`
> application. This is the real-time inference stage of the prediction pipeline.
> It consumes a Kafka stream of CS2 GSI events, computes the 10 model features
> (including stateful prev3 kill/damage history), applies the trained
> `PipelineModel`, and publishes CT win probabilities to an output Kafka topic.

---

## 1. Role in the system

```
Kafka topic: cs2.gsi.events       (produced by cs2-gsi-bridge)
    │
    ▼
cs2-predictor-inference            ← THIS APPLICATION
    │
    │  Step A: parse + filter GSI JSON
    │  Step B: extract stateless features (ct_alive, t_alive, hp, bomb, time)
    │  Step C: compute stateful prev3 features (flatMapGroupsWithState)
    │  Step D: apply PipelineModel.transform()
    │
    ▼
Kafka topic: cs2.gsi.predictions
    JSON: { "sessionKey": "...", "round": N, "timestamp": T, "probCtWin": 0.73 }
```

The trained `PipelineModel` from the trainer repo (`pipeline-model/` artifact)
is loaded once at startup and used for every micro-batch. No retraining happens
here.

---

## 2. Technology stack

### Decided

| Layer | Choice |
| --- | --- |
| Language | Java 17 |
| Build | Apache Maven |
| Streaming framework | Apache Spark 4.1.0 Structured Streaming (same as trainer) |
| Kafka connector | `spark-sql-kafka-0-10_2.13` (Spark's official Kafka source/sink) |
| JSON parsing | Spark's built-in `from_json` + `StructType` for stateless path; Jackson (in state function) for stateful path |
| Config | YAML via Jackson (`jackson-dataformat-yaml`) |
| Logging | log4j2 / SLF4J (same as trainer) |
| Testing | JUnit 5 |

### Deliberately rejected

- **Kafka Streams.** We need Spark's `PipelineModel.transform()` which operates
  on Spark DataFrames. Kafka Streams would require exporting the model to another
  format (PMML, ONNX) — added complexity with no benefit.
- **Flink.** Same rationale as Kafka Streams. Keeping everything in Spark means
  the `PipelineModel` artifact is consumed directly.
- **Continuous processing mode.** Spark's experimental continuous mode does not
  support stateful operators (`flatMapGroupsWithState`). Micro-batch mode is used.

### Hardware target (local development)

Same machine as the trainer (Ryzen 7 5700X). `local[4]` is sufficient for a
single-client stream. The trigger interval should be `1 second` to match CS2
GSI's `throttle: 0.5` setting (one event per ~0.5 s → at most 2 events/trigger).

---

## 3. Feature mapping: GSI JSON → model features

The trainer expects these 10 feature columns (from `FeatureColumns`):

| Feature column | Source in GSI JSON | Notes |
| --- | --- | --- |
| `ct_alive` | `count(allplayers where team="CT" and state.health > 0)` | |
| `t_alive` | `count(allplayers where team="T" and state.health > 0)` | |
| `ct_total_hp` | `sum(state.health for CT players with health > 0)` | |
| `t_total_hp` | `sum(state.health for T players with health > 0)` | |
| `bomb_planted` | `1 if round.phase == "bomb" else 0` | |
| `remaining_time` | `parseFloat(phase_countdowns.phase_ends_in)` | Pre-plant = round timer; post-plant = bomb fuse. Valid for phase "live" and "bomb". |
| `ct_alive_kills_prev3_sum` | Sum of `kills_in_round` for currently-alive CT players, over previous 3 completed rounds | **Stateful** — see Section 4 |
| `ct_alive_damage_prev3_sum` | Sum of `damage_in_round` for currently-alive CT players, over previous 3 completed rounds | **Stateful** |
| `t_alive_kills_prev3_sum` | Same for T-side alive players | **Stateful** |
| `t_alive_damage_prev3_sum` | Same for T-side alive players | **Stateful** |

### Phase filtering

Only predict when `round.phase ∈ {"live", "bomb"}`. Events with
`round.phase == "freezetime"` or `"over"` pass through the stateful operator
(to update per-round history) but do not produce prediction output rows.

---

## 4. Stateful prev3 computation

This is the most complex part of the application. Per-round kill and damage
stats must be tracked per-player across consecutive rounds.

### Why it's hard

- GSI does not send a dedicated "round ended" event. Round transitions are
  inferred from phase changes.
- `match_stats.kills` is cumulative over the whole match (does not reset per round).
- `state.round_totaldmg` resets to 0 at the start of each new round.

### Per-round stat derivation

**Kills per round for player P in round N:**
```
kills_in_round_N(P) = P.match_stats.kills           (at round N end)
                    − P.kills_at_start_of_round_N   (saved in state when round N began)
```

**Damage per round for player P in round N:**
```
damage_in_round_N(P) = P.state.round_totaldmg       (read during "over" phase)
```
`round_totaldmg` still contains the current round's damage when `round.phase == "over"`.
It resets to 0 when the next round's `freezetime` or `live` begins.

### Round lifecycle detected from GSI phase sequence

```
... → "live" / "bomb"  (gameplay)
    → "over"           (round just ended; round_totaldmg still valid here → ARCHIVE)
    → "freezetime"     (new round starting; match_stats.kills updated → SAVE ROUND START)
    → "live"           (new round active → PREDICT)
    → ...
```

Events to act on in the state function:

| Detected transition | Action |
| --- | --- |
| `round.phase == "over"` with `map.round == currentRound` | Archive each player's `{kills: cumulative - start, damage: round_totaldmg}` into their deque (max 3). |
| `round.phase == "freezetime"` with `map.round > currentRound` | Save `kills_at_round_start[player] = match_stats.kills` for every player in `allplayers`. Advance `currentRound`. |
| `round.phase ∈ {"live", "bomb"}` | Compute features, emit output row. |

### State schema (per match session)

```java
class MatchSessionState implements Serializable {
    int  currentRound;                           // last round we've processed
    boolean roundArchived;                       // did we archive "over" for currentRound?
    Map<String, Integer> killsAtRoundStart;      // steamId → cumulative kills at start
    Map<String, Deque<RoundStats>> playerHistory;// steamId → last 3 rounds of stats
}

record RoundStats(int kills, int damage) implements Serializable {}
```

### Prev3 sum computation

For a given live tick, the currently-alive CT players are all players in
`allplayers` where `team="CT"` and `state.health > 0`. For each alive player P:

```
ct_alive_kills_prev3_sum += sum of playerHistory[P.steamId].kills for up to 3 entries
ct_alive_damage_prev3_sum += sum of playerHistory[P.steamId].damage for up to 3 entries
```

If a player has fewer than 3 rounds of history (early in the match), sum what's
available. If a player is not in `playerHistory` (joined mid-match or is a bot
added late), their contribution is 0.

### Grouping key

All GSI messages are grouped by the Kafka record key, which the bridge sets to
`provider.steamid`. This routes all events from the same game server to the same
Spark task and provides the session identifier for state management.

In `flatMapGroupsWithState`, the group key is the Kafka key string.

---

## 5. Architecture

### Module layout

```
cs2-predictor-inference/
├── pom.xml
├── CLAUDE.md                                         # this file
├── config/
│   └── inference-config.yaml                        # default config
└── src/
    ├── main/java/com/cs2predictor/inference/
    │   ├── Cs2InferenceApplication.java              # entry point
    │   ├── config/                                   # YAML config records
    │   │   ├── InferenceConfig.java                  # root record
    │   │   ├── SparkConfig.java
    │   │   ├── KafkaInputConfig.java                 # bootstrapServers, topic, startingOffsets
    │   │   ├── KafkaOutputConfig.java                # bootstrapServers, topic
    │   │   ├── KafkaSecurityConfig.java              # SASL/TLS — shared by all Kafka clients
    │   │   ├── ModelConfig.java                      # pipelineModelPath
    │   │   └── ConfigLoader.java
    │   ├── spark/
    │   │   └── SparkSessionFactory.java
    │   ├── model/
    │   │   └── ModelLoader.java                      # PipelineModel.load(path)
    │   ├── schema/
    │   │   └── GsiSchema.java                        # Spark StructType for GSI JSON
    │   ├── stream/
    │   │   ├── GsiStreamReader.java                  # readStream from Kafka → raw JSON rows
    │   │   ├── GsiEventParser.java                   # from_json → structured columns
    │   │   ├── StatelessFeatureExtractor.java        # ct_alive, t_alive, hp, bomb, time
    │   │   ├── SessionStateFunction.java             # flatMapGroupsWithState logic + MatchSessionState
    │   │   └── PredictionSink.java                   # write predictions to Kafka / console
    │   └── util/
    │       └── AlivePlayerAggregator.java            # helper: aggregate allplayers map per tick
    ├── resources/
    │   └── log4j2.xml                                # logging config (root=ERROR, app=INFO)
    └── test/java/com/cs2predictor/inference/
        ├── stream/
        │   └── SessionStateFunctionTest.java         # unit tests for state transitions
        └── schema/
            └── GsiSchemaTest.java                    # verify parsing of gsi-example.txt
```

### Data flow through the pipeline

```
Kafka source (cs2.gsi.events)
    │  key=steamId, value=raw GSI JSON string
    │
    ▼
GsiStreamReader
    │  Dataset<Row>: key (string), value (string), timestamp
    │
    ▼
GsiEventParser                   (from_json + select)
    │  columns: session_key, round_num, round_phase,
    │            phase_ends_in, allplayers_json (string),
    │            bomb_phase
    │
    ▼
groupByKey("session_key")
    │
    ▼
SessionStateFunction             (flatMapGroupsWithState)
    │  stateful: maintains MatchSessionState per session
    │  emits rows only when round_phase ∈ {live, bomb}
    │  columns: session_key, round_num, event_timestamp,
    │            ct_alive, t_alive, ct_total_hp, t_total_hp,
    │            bomb_planted, remaining_time,
    │            ct_alive_kills_prev3_sum, ct_alive_damage_prev3_sum,
    │            t_alive_kills_prev3_sum,  t_alive_damage_prev3_sum
    │
    ▼
ModelLoader.getModel().transform(df)
    │  adds: features (vector), scaled_features (vector),
    │         probability (vector), prediction (double)
    │
    ▼
PredictionSink                   (select + to_json + writeStream to Kafka)
    │  key=session_key, value={"sessionKey":..., "round":N, "probCtWin":0.73, "timestamp":...}
    ▼
Kafka topic: cs2.gsi.predictions
```

---

## 6. GsiSchema: parsing the GSI JSON with Spark

Only the fields needed for inference are extracted. The full schema is not
modelled — unknown keys are silently ignored by `from_json`.

```java
// Relevant subset of the GSI JSON schema
StructType playerStateSchema = new StructType()
    .add("health",         DataTypes.IntegerType)
    .add("round_kills",    DataTypes.IntegerType)
    .add("round_totaldmg", DataTypes.IntegerType);

StructType playerMatchStatsSchema = new StructType()
    .add("kills", DataTypes.IntegerType);

StructType playerSchema = new StructType()
    .add("team",         DataTypes.StringType)
    .add("state",        playerStateSchema)
    .add("match_stats",  playerMatchStatsSchema);

StructType roundSchema = new StructType()
    .add("phase", DataTypes.StringType);

StructType phaseCountdownsSchema = new StructType()
    .add("phase",         DataTypes.StringType)
    .add("phase_ends_in", DataTypes.StringType);   // float as string

StructType mapSchema = new StructType()
    .add("round", DataTypes.IntegerType)
    .add("phase", DataTypes.StringType);

StructType providerSchema = new StructType()
    .add("steamid", DataTypes.StringType);

StructType gsiSchema = new StructType()
    .add("provider",          providerSchema)
    .add("map",               mapSchema)
    .add("round",             roundSchema)
    .add("phase_countdowns",  phaseCountdownsSchema)
    .add("allplayers",        MapType(DataTypes.StringType, playerSchema));
```

The `allplayers` field parsed as `MapType(StringType, playerSchema)` allows
column-level access via Spark's `map_values()`, `explode()`, etc. for the
stateless features. For the stateful function, the raw JSON string of
`allplayers` is re-parsed with Jackson inside the state function (simpler
than passing Spark's internal `MapData` through the state serialization boundary).

---

## 7. Implementation steps

### ✅ Step 1: Spark bootstrap + Kafka source

Minimal streaming application: create SparkSession, read from Kafka topic,
print raw values to console with a 1-second trigger. Confirms Kafka connectivity
and Spark streaming bootstrap.

**Files:**
- `pom.xml` — `spark-core`, `spark-sql`, `spark-mllib`, `spark-sql-kafka-0-10`,
  Jackson YAML, JUnit 5; Java 17 module opens same as trainer
- `Cs2InferenceApplication.java` — `main()`: load config, create Spark, start stream
- All `config/` records + `ConfigLoader.java`
- `config/inference-config.yaml`
- `spark/SparkSessionFactory.java` (copy + adapt from trainer)
- `stream/GsiStreamReader.java` — `readStream().format("kafka")...`

### ✅ Step 2: JSON parsing and schema

Parse the raw Kafka value strings using `from_json(col("value"), gsiSchema)`.
Extract `session_key` (= Kafka key / provider.steamid), `round_num`,
`round_phase`, `phase_ends_in`, and `allplayers`. Filter out rows where
`map` is null (non-game messages that slipped through the bridge).

**Files:**
- `schema/GsiSchema.java` — `public static StructType get()`
- `stream/GsiEventParser.java` — `public static Dataset<Row> parse(Dataset<Row> raw)`
- `schema/GsiSchemaTest.java` — parse `gsi-example.txt`, assert key fields extracted

### ✅ Step 3: Stateless feature extraction

Implement `StatelessFeatureExtractor.extract(Dataset<Row> parsed)`. Explode the
`allplayers` map into per-player rows, filter/aggregate to get:
- `ct_alive`, `t_alive`, `ct_total_hp`, `t_total_hp`
- `bomb_planted` (1 if `round_phase == "bomb"`)
- `remaining_time` (cast `phase_ends_in` string to float)

This produces one row per GSI event with all 6 stateless features alongside the
raw `allplayers` JSON string for the stateful step.

**Files:**
- `stream/StatelessFeatureExtractor.java`
- `util/AlivePlayerAggregator.java` (if the aggregation logic warrants a helper)

### ✅ Step 4: Stateful prev3 computation

Implement `SessionStateFunction` as a
`FlatMapGroupsWithStateFunction<String, Row, MatchSessionState, Row>`.

The function processes each micro-batch of rows for one session key. For each
row (sorted by Kafka offset):
1. If `round_phase == "over"` and not yet archived for this round:
   - Parse `allplayers` JSON with Jackson
   - For each player: compute `kills = match_stats.kills − killsAtRoundStart`
   - Save `{kills, damage=round_totaldmg}` to `playerHistory` deque (max 3)
   - Mark `roundArchived = true`
2. If `round_phase == "freezetime"` and `round_num > currentRound`:
   - Parse `allplayers` JSON, save `killsAtRoundStart[steamId] = match_stats.kills`
   - Advance `currentRound`, reset `roundArchived = false`
3. If `round_phase ∈ {"live", "bomb"}`:
   - Parse `allplayers` JSON, compute prev3 sums for alive CT and T players
   - Emit a complete feature row

Set `GroupStateTimeout.ProcessingTimeTimeout()` with a timeout of 4 hours (one
match can last ~90 minutes; 4 hours provides a safe margin). On timeout,
remove the state.

**Files:**
- `stream/SessionStateFunction.java` — implements the state function + defines
  `MatchSessionState` and `RoundStats` inner classes
- `stream/SessionStateFunctionTest.java` — unit tests:
  - First 3 rounds: prev3 sums grow incrementally
  - Round 4+: sliding window of exactly 3 rounds
  - Player mid-match join: no history → contributes 0
  - Freezetime-only messages: no output rows emitted
  - "over" followed by new round: round archived correctly

### ✅ Step 5: Inference

Load the `PipelineModel` from `config.model.pipelineModelPath` at startup via
`ModelLoader`. After the state function emits complete feature rows, call
`model.transform(featuresDf)`. Extract `prob_ct_win` via
`vector_to_array(probability)[1]`. Select and rename the output columns.

Note: `PipelineModel.transform()` operates on a Spark DataFrame — the model is
broadcast to all executors and applied in parallel across micro-batches.
The model's `VectorAssembler` expects exactly the 10 feature column names from
`FeatureColumns`; the streaming schema must match.

**Files:**
- `model/ModelLoader.java` — `public static PipelineModel load(ModelConfig config, SparkSession spark)`

### ✅ Step 6: Output sink

Implement `PredictionSink.write(Dataset<Row> predictions, InferenceConfig config)`.
Serialize each prediction row to JSON:
```json
{
  "sessionKey": "76561198329629332",
  "round": 2,
  "probCtWin": 0.7341,
  "timestamp": 1778534359000
}
```
Write to the configured Kafka output topic (key = `session_key`).

For development, support `output.mode = "console"` that writes to stdout
instead of Kafka.

**Files:**
- `stream/PredictionSink.java`

---

## 8. Configuration reference

Default `inference-config.yaml`:

```yaml
spark:
  appName: "cs2-predictor-inference"
  master: "local[4]"
  shufflePartitions: 4
  triggerIntervalMs: 1000        # micro-batch interval

kafkaInput:
  bootstrapServers: "localhost:9092"
  topic: "cs2.gsi.events"
  startingOffsets: "latest"      # "earliest" for replay

kafkaOutput:
  bootstrapServers: "localhost:9092"
  topic: "cs2.gsi.predictions"

# Optional — omit entirely for a plaintext local broker (defaults to PLAINTEXT)
kafkaSecurity:
  securityProtocol: "SASL_SSL"
  saslMechanism: "SCRAM-SHA-256"
  saslUsername: "..."
  saslPassword: "..."

model:
  pipelineModelPath: "../cs2-predictor/models/cs2-predictor/pipeline-model"

output:
  mode: "kafka"                  # "kafka" or "console"
```

`KafkaSecurityConfig` defaults `securityProtocol` to `PLAINTEXT` and treats SASL as
disabled when `saslMechanism` is absent, so the `kafkaSecurity` block is optional for
local development.

---

## 9. Constraints and gotchas

### `flatMapGroupsWithState` requires Append output mode
The stream must use `OutputMode.Append` throughout. `Complete` mode is
incompatible with stateful operations.

### The `allplayers` map is re-parsed with Jackson in the state function
Passing Spark's `MapData`/`ArrayData` types through the Java state serialization
boundary (Kryo) is error-prone. It is simpler to keep `allplayers` as a raw
JSON string column and re-parse it with Jackson inside the state function.
This adds ~0.5 ms per event but eliminates a category of serialization bugs.

### `match_stats.kills` is match-cumulative; `round_totaldmg` is per-round
See Section 4. Never use `round_kills` from `state` for prev3 — it resets and
cannot be recovered from the top-level `allplayers` once the round ends.
`match_stats.kills` (cumulative) minus `killsAtRoundStart` is the correct
per-round kill derivation.

### Row encoder for `flatMapGroupsWithState` output in Spark 4.x

`RowEncoder.apply(schema)` was removed in Spark 4.x and `Encoders.row(schema)` is not yet a
public API in 4.1. The workaround: emit JSON strings from `StateFunction`
(`Encoders.STRING()` as output encoder) and parse them back into a typed `Dataset<Row>` with
`from_json(col("_json"), OUTPUT_SCHEMA)` in `SessionStateFunction.apply()`. One extra
serialisation step per emitted row (~13 fields), negligible overhead.
`event_timestamp` is stored as epoch-milliseconds (`LongType`) in the output schema so it
survives the JSON round-trip without custom timestamp parsing.

### Spark Structured Streaming with `flatMapGroupsWithState` and Kafka source requires checkpointing
Set `option("checkpointLocation", ...)` on the `writeStream`. Without a
checkpoint, the streaming job cannot recover state across restarts.

### Session timeout must exceed match length
Set the state timeout to at least 2–3 hours. If a session times out during a
match (e.g. after a long pause or disconnect), the state is dropped and prev3
history restarts from zero when events resume.

### Java 17 module flags
Same `--add-opens` flags as the trainer are required. Wire them into both
`maven-surefire-plugin` and `exec-maven-plugin` in `pom.xml`.

### `spark.driver.memory` is ignored in local mode
Use `-Xmx` in the exec plugin arguments.

### Prediction output schema
The `PipelineModel` adds `features`, `scaled_features`, `probability`, and
`prediction` columns. Select only the columns needed for the output JSON
before writing to Kafka to avoid serializing internal Spark ML vectors.

### Phase ordering within a micro-batch
Events in a micro-batch arrive in Kafka offset order per partition. Since all
events for a session key are in the same Kafka partition (by key-based routing),
offset order equals arrival order. Sort by offset (or by `provider.timestamp`)
inside the state function before processing to handle any minor reordering.

### Checkpoint is cleared on every startup
`Cs2InferenceApplication.clearCheckpoints()` deletes `checkpoint/predictions-kafka`
and `checkpoint/predictions-console` before the SparkSession is created. This gives
fresh state on every run (prev3 history starts from zero). Uses `Files.walkFileTree`
with `Files.delete()` — not `File.delete()`, which silently fails on Windows for
dot-prefixed files (`.N.delta.crc`) left by the HDFSBackedStateStore.

Spark 4.x enforces `STATE_STORE_CHECKPOINT_LOCATION_NOT_EMPTY` on batch 0: if any
stale state files remain, the query fails immediately. The `walkFileTree` approach
avoids this.

### SASL_SSL for Redpanda Serverless (and other managed brokers)
All three Kafka client sites apply security options from `KafkaSecurityConfig`:
- **Spark readStream** (`GsiStreamReader`) — `kafka.security.protocol`,
  `kafka.sasl.mechanism`, `kafka.sasl.jaas.config` options (note `kafka.` prefix
  required by Spark's Kafka connector)
- **Spark writeStream** (`PredictionSink`) — same `kafka.*` options on the writer
- **Plain Kafka client** (`ManualRequestConsumer`) — `security.protocol`,
  `sasl.mechanism`, `sasl.jaas.config` properties (no prefix)

The JAAS config string is built from `saslUsername` + `saslPassword` in
`KafkaSecurityConfig.buildJaasConfig()` — credentials are never hardcoded.

### Logging
`src/main/resources/log4j2.xml` configures:
- `com.cs2predictor` → `INFO` (all application logs visible)
- `org.apache.spark.sql.execution.streaming.StreamExecution` → `INFO` (batch progress)
- All other framework loggers (`org.apache.spark`, `org.apache.hadoop`, `org.apache.kafka`,
  `io.netty`, `org.apache.parquet`, `org.apache.zookeeper`) → `ERROR`
- Root → `ERROR`

---

## 10. Related documents

- `GSI_BRIDGE_DESIGN.md` — upstream Kafka bridge design (this repo)
- `CLAUDE.md` — trainer application context (this repo)
- `PARQUET_SCHEMA.md` — training feature schema; inference must match exactly
- `gsi-example.txt` — sample GSI payload used in schema + filter tests
- `models/cs2-predictor/pipeline-model/` — the deployment artifact produced
  by the trainer; load path set in `inference-config.yaml`
