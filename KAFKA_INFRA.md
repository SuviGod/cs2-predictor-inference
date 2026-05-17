# Kafka Infrastructure — CS2 Predictor Pipeline

Single-node **Redpanda** broker running in Docker. Used by both the GSI bridge
(producer) and the inference app (consumer + producer).

---

## Starting the broker

```bash
docker run -d --name redpanda-cs2 -p 9092:9092 \
  redpandadata/redpanda:latest \
  redpanda start \
    --overprovisioned --smp 1 --memory 512M --reserve-memory 0M \
    --node-id 0 --check=false \
    --kafka-addr 0.0.0.0:9092 \
    --advertise-kafka-addr localhost:9092
```

After startup, verify with:

```bash
docker exec redpanda-cs2 rpk cluster health
```

---

## Connection

| Property           | Value          |
|--------------------|----------------|
| `bootstrap.servers`| `localhost:9092` |
| Security           | PLAINTEXT (no auth) |
| Kafka API version  | 3.x compatible |

---

## Topics

### `cs2.gsi.events` — GSI bridge output / inference app input

Created by the GSI bridge. **The inference app consumes from this topic.**

```bash
# already exists — do not re-create
docker exec redpanda-cs2 rpk topic describe cs2.gsi.events
```

| Property    | Value |
|-------------|-------|
| Partitions  | 1     |
| Replicas    | 1     |
| Retention   | 7 days (default) |

**Message format:**

| Field | Value |
|-------|-------|
| Key   | `provider.steamid` (String) — routes all events from the same game server to the same partition; required for stateful session ordering |
| Value | Raw GSI JSON body (String, UTF-8) — forwarded unchanged from CS2 |
| Headers | none |

Relevant top-level JSON fields the inference app can rely on:

```
provider.steamid    — partition key / session identifier
map.mode            — always "competitive" (bridge filters non-competitive by default)
map.phase           — always "live" (bridge filters warmup/halftime/gameover)
map.name            — map identifier, e.g. "de_dust2"
map.round           — current round number
allplayers          — object keyed by player SteamID; always present and non-empty
round.phase         — "freezetime" | "live" | "over" (bridge does NOT filter on this)
previously          — diff object (fields that changed from last tick); may be absent
```

### `cs2.gsi.predictions` — inference app output

**The inference app must create this topic.** Suggested configuration:

```bash
docker exec redpanda-cs2 rpk topic create cs2.gsi.predictions \
  --partitions 1 --replicas 1
```

Expected message convention (to be finalised by the inference app):

| Field | Suggested value |
|-------|-----------------|
| Key   | `provider.steamid` (same as input — keeps prediction and event in the same partition order) |
| Value | JSON with prediction results + source event metadata |

---

## Useful `rpk` commands

```bash
# list all topics
docker exec redpanda-cs2 rpk topic list

# consume all messages from the beginning
docker exec redpanda-cs2 rpk topic consume cs2.gsi.events --offset start

# tail live messages
docker exec redpanda-cs2 rpk topic consume cs2.gsi.events

# show message count / partition offsets
docker exec redpanda-cs2 rpk topic describe cs2.gsi.events

# delete topic (if you need to reset)
docker exec redpanda-cs2 rpk topic delete cs2.gsi.events
```

---

## Notes

- This is a **development-only** single-node setup with no replication or persistence
  beyond the container lifetime. Stopping and removing the container loses all messages.
- The GSI bridge config that points to this broker lives in
  `config/bridge-config.yaml` (`kafka.bootstrapServers`).
- The bridge uses `acks=1`, `linger.ms=5`, `compression.type=lz4`.
- The inference app consumer group ID is `cs2-predictor-inference` (set via `kafkaInput.groupId` in `inference-config.yaml`).
  Monitor with: `docker exec redpanda-cs2 rpk group describe cs2-predictor-inference`