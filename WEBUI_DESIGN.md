# CS2 Predictor — Web UI Design

> **Purpose:** Design for a real-time browser dashboard that reads predictions from
> `cs2.gsi.predictions`, displays live probability, time-series charts, per-round
> history, and lets users manually submit a feature set for an ad-hoc prediction.

---

## Progress

| Step | Component | Status |
|---|---|---|
| 1 | Java manual prediction consumer (`ManualRequestConsumer`) | ✅ Done |
| 2 | Node.js web server (`cs2-predictor-ui/server.js`) | ✅ Done |
| 3 | Frontend (`index.html`, `app.js`, `style.css`) | ✅ Done |
| 4 | End-to-end wiring and smoke test | ✅ Done |
| 5 | Docker image (`cs2-predictor-ui/Dockerfile`) | ✅ Done |
| 6 | Clear history button (`DELETE /api/history`, `CLEAR_KEY`) | ✅ Done |
| 7 | Ghost consumer fix (`CONSUMER_GROUP` env var) | ✅ Done |
| 8 | AWS EC2 deployment (t3.micro, AMI, security group, SSH key) | ✅ Done |

---

## 1. Architecture overview

```
cs2.gsi.predictions (Kafka)
        │
        ▼
cs2-predictor-ui/server.js          ← Node.js + Express + kafkajs
        │  in-memory store (history + per-round map)
        ├── GET /                   → serves index.html
        ├── GET /api/history        → full history snapshot (JSON)
        ├── GET /api/events         → SSE stream (live updates)
        └── POST /api/predict       → publishes to cs2.gsi.manual.requests
                                            │
                                    cs2.gsi.manual.requests (Kafka)
                                            │
                                            ▼
                                   ManualRequestConsumer (Java daemon thread)
                                   spark.newSession()
                                   + PipelineModel.transform()
                                            │
                                    cs2.gsi.manual.responses (Kafka)
                                            │
                                            ▼
                                   server.js (correlated response)
                                            │
                                            ▼
                                   { "probCtWin": 0.73 }

Browser ←── SSE (live ticks) + HTTP (history, manual)
```

| Component | Location | New dependency | Status |
|---|---|---|---|
| Java manual consumer | `cs2-predictor-inference` (daemon thread) | None (existing Jackson + Kafka client) | ✅ Done |
| Node.js web server | New directory `cs2-predictor-ui/` | `express`, `kafkajs` | ✅ Done |
| Frontend | `cs2-predictor-ui/public/` | Chart.js, Bootstrap 5 (CDN) | ✅ Done |
| Docker image | `cs2-predictor-ui/Dockerfile` | None | ✅ Done |

---

## 2. UI layout

```
┌──────────────────────────────────────────────────────────────┐
│  CS2 Round Outcome Predictor                    ● LIVE        │
├──────────────────────┬───────────────────────────────────────┤
│  LATEST PREDICTION   │  PROBABILITY OVER TIME                │
│                      │                                       │
│   CT Win             │   1.0 ┤         ╭──╮                  │
│   ████████▌  73%     │   0.5 ┤──╮  ╭───╯  ╰──               │
│                      │   0.0 ┤  ╰──╯                        │
│   Round  8           │       └────────────────────── time    │
│   Session 7656…      │       [zoom / pan via Chart.js zoom]  │
├──────────────────────┴───────────────────────────────────────┤
│  PER-ROUND HISTORY                                           │
│  ▶  Round 1    CT: 68%  T: 32%  (12 ticks)                  │
│  ▶  Round 2    CT: 55%  T: 45%  (9 ticks)                   │
│  ▼  Round 8    CT: 73%  T: 27%  (current)                   │
│     ┌───────────────────────────────────────────────┐       │
│     │  1.0 ┤      ╭───╮                              │       │
│     │  0.5 ┤──╮───╯   ╰──────                        │       │
│     │      └──────────────────────────── tick #      │       │
│     └───────────────────────────────────────────────┘       │
├──────────────────────────────────────────────────────────────┤
│  MANUAL PREDICTION                                           │
│  CT alive [3▾]  T alive [2▾]  CT HP [250]  T HP [180]       │
│  Bomb planted [No▾]  Time left [45.0]                        │
│  CT kills prev3 [4]  CT dmg prev3 [320]                      │
│  T kills prev3 [2]   T dmg prev3 [150]                       │
│                                          [Predict →]         │
│  Result:  CT Win probability: 0.67                           │
└──────────────────────────────────────────────────────────────┘
```

---

## 3. Kafka message format (existing contract)

Messages on `cs2.gsi.predictions`:
```json
{
  "sessionKey": "76561198329629332",
  "round": 8,
  "probCtWin": 0.7341,
  "timestamp": 1778844059000
}
```

---

## 4. Java manual prediction consumer (`ManualRequestConsumer`) — ✅ DONE

Manual predictions use a Kafka request-response pattern instead of a direct REST call.
This avoids exposing an extra HTTP port from the inference JVM.

### Kafka topics

| Topic | Direction | Description |
|---|---|---|
| `cs2.gsi.manual.requests` | UI → Java | Feature set + correlationId |
| `cs2.gsi.manual.responses` | Java → UI | `probCtWin` + correlationId |

### Request message (published by `server.js`)
```json
{
  "correlationId": "<uuid>",
  "ctAlive":            3,
  "tAlive":             2,
  "ctTotalHp":          250,
  "tTotalHp":           180,
  "bombPlanted":        0,
  "remainingTime":      45.0,
  "ctAliveKillsPrev3":  4,
  "ctAliveDamagePrev3": 320,
  "tAliveKillsPrev3":   2,
  "tAliveDamagePrev3":  150
}
```

### Response message (consumed by `server.js`)
```json
{ "correlationId": "<uuid>", "probCtWin": 0.7564 }
```

**Error response:**
```json
{ "correlationId": "<uuid>", "error": "reason string" }
```

### Implementation notes

- `ManualRequestConsumer` runs as a daemon thread started in `Cs2InferenceApplication.main()`
  after `ModelLoader.load()`.
- Uses a plain `KafkaConsumer` / `KafkaProducer` (not Spark Streaming) — simpler and lower
  latency for single-row batch inference.
- `spark.newSession()` shares the underlying `SparkContext` (thread-safe) but isolates SQL
  configuration. Safe to call concurrently with the streaming query.
- `FEATURE_SCHEMA` column names exactly match the `VectorAssembler`'s `inputCols` in the
  trained `PipelineModel`. Column order in the schema is irrelevant; the assembler selects
  by name.
- `server.js` uses a `pendingRequests` Map (correlationId → `{resolve, reject, timeoutId}`)
  to match responses to waiting HTTP clients. Requests time out after 10 seconds.

---

## 5. Node.js web server — ✅ DONE

### Directory layout
```
cs2-predictor-ui/
├── package.json
├── package-lock.json
├── Dockerfile
├── .dockerignore
├── server.js                 ← Express app + Kafka consumer + SSE
└── public/
    ├── index.html            ← Single-page UI
    ├── app.js                ← Frontend logic (SSE, charts, form)
    └── style.css             ← Minimal custom styles on top of Bootstrap
```

### Dependencies (`package.json`)
```json
{
  "dependencies": {
    "express": "^4.19.0",
    "kafkajs": "^2.2.4"
  }
}
```
No build step. Run with `node server.js`.

### In-memory data store (`server.js`)

```js
// All predictions ever received in this server session
const allPredictions = [];          // [{sessionKey, round, probCtWin, timestamp}, ...]

// Per-round tick arrays: Map<sessionKey → Map<round → {probCtWin, timestamp}[]>>
const roundHistory = new Map();
```

`allPredictions` is unbounded in memory. For a dev session that is acceptable;
the Notes section explains how to add a rolling cap if needed.

### Kafka consumers (inside `server.js`)

```js
const kafka = new Kafka({
  brokers: KAFKA_BROKERS.split(','),
  clientId: 'cs2-ui',
  connectionTimeout: 3_000,   // needed for cloud brokers
  requestTimeout:    30_000,
  retry: { retries: 5 },
  // ssl + sasl injected when KAFKA_SASL_MECHANISM is set
});

const CONSUMER_GROUP_CONFIG = { sessionTimeout: 30_000, heartbeatInterval: 3_000 };

// Live predictions — fromBeginning: false avoids stalled-offset issue on Redpanda Serverless
const consumer = kafka.consumer({ groupId: 'cs2-predictor-ui', ...CONSUMER_GROUP_CONFIG });
await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

// Manual prediction responses — latest only (responses are transient)
const responseConsumer = kafka.consumer({ groupId: 'cs2-predictor-ui-responses', ...CONSUMER_GROUP_CONFIG });
await responseConsumer.subscribe({ topic: MANUAL_RESPONSE_TOPIC, fromBeginning: false });
```

### HTTP endpoints (`server.js`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Serves `public/index.html` |
| `GET` | `/api/history` | Returns `{ predictions: allPredictions, roundHistory }` as JSON |
| `GET` | `/api/events` | SSE stream; sends `data: <json>\n\n` per new prediction |
| `POST` | `/api/predict` | Publishes to `cs2.gsi.manual.requests`, awaits correlated response (10 s timeout) |

### Configuration (top of `server.js`)
```js
const WEB_PORT              = process.env.WEB_PORT              || 3000;
const KAFKA_BROKERS         = process.env.KAFKA_BROKERS         || 'localhost:9092';
const KAFKA_TOPIC           = process.env.KAFKA_TOPIC           || 'cs2.gsi.predictions';
const MANUAL_REQUEST_TOPIC  = process.env.MANUAL_REQUEST_TOPIC  || 'cs2.gsi.manual.requests';
const MANUAL_RESPONSE_TOPIC = process.env.MANUAL_RESPONSE_TOPIC || 'cs2.gsi.manual.responses';
```

---

## 6. Frontend (`public/index.html` + `public/app.js`) — ✅ DONE

### Libraries (CDN, no build step)
- **Bootstrap 5.3** — layout and accordion
- **Chart.js 4** — time-series and per-round mini charts
- **chartjs-plugin-zoom** (+ Hammer.js) — pinch/scroll zoom on the main chart

### Sections and their JS behaviour

#### 6.1 Latest prediction panel
- Updated on every SSE message.
- Colour-coded: green if `probCtWin > 0.5`, red if `< 0.5`.
- Shows `sessionKey` (last 8 chars), `round`, `probCtWin` as percentage.

#### 6.2 Probability over time (main chart)
- Chart.js `line` chart, x-axis `time` scale (epoch ms), y-axis 0–1.
- Data source: seeded from `GET /api/history` on page load, then each SSE event
  appends a point — no page refresh needed.
- Chart is unbounded: all points are kept; Chart.js renders a scrollable/zoomable
  view via `chartjs-plugin-zoom` (wheel to zoom, drag to pan).
- One dataset per `sessionKey` so multiple concurrent sessions appear as separate
  coloured lines.

#### 6.3 Per-round history (accordion)
- Bootstrap accordion, one item per round number (across all sessions).
- Each accordion header shows: round number, last `probCtWin`, tick count.
- Expanded body: a Chart.js mini `line` chart showing `probCtWin` vs tick index
  for that round. X-axis is tick count (1, 2, 3…), not wall-clock time, so slow
  rounds and fast rounds compare fairly.
- Accordion items are added dynamically as new rounds appear; the current round's
  item is always open.

#### 6.4 Clear history button

- Red outline button in the navbar (top-right, next to the live status dot).
- On click: `prompt()` asks for the secret key (configured via `CLEAR_KEY` env var, default `cs2clear`).
- Sends `DELETE /api/history` with `x-clear-key: <key>` header.
- Server verifies key, clears `allPredictions` and `roundHistory`, then broadcasts `{ type: 'clear' }` via SSE.
- All connected browser tabs receive the SSE clear event and call `clearUI()`, which resets the main chart, latest panel, and accordion simultaneously.
- Wrong key → `alert('Wrong key.')`. Cancelled prompt → no request sent.

#### 6.5 Manual prediction form
- Ten labelled inputs (number fields with sensible min/max/step):
  `CT alive` (0–5), `T alive` (0–5), `CT total HP` (0–500),
  `T total HP` (0–500), `Bomb planted` (checkbox → 0/1),
  `Time remaining` (0–115, step 0.1),
  `CT kills prev3` (0–), `CT damage prev3` (0–),
  `T kills prev3` (0–), `T damage prev3` (0–).
- Submit button calls `POST /api/predict` on the Node.js server (which publishes
  to Kafka and awaits the correlated response). Shows spinner while waiting.
- Result displayed below the form: probability bar + percentage.
- Pre-fill button: copies the values from the latest live prediction into the form
  so the user can tweak a real game state.

---

## 7. Docker — ✅ DONE

### `Dockerfile`

```dockerfile
FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY server.js ./
COPY public/ ./public/
EXPOSE 3000
CMD ["node", "server.js"]
```

### Build and run

```bash
docker build -t cs2-predictor-ui ./cs2-predictor-ui

docker run -p 3000:3000 \
  -e KAFKA_BROKERS=host.docker.internal:9092 \
  cs2-predictor-ui
```

### Kafka advertised listener requirement

When the UI runs inside Docker it reaches Kafka via `host.docker.internal:9092`.
Kafka responds to the bootstrap connection with its *advertised listener* address.
If the broker advertises `localhost:9092` (the default for a local install), KafkaJS
will try to connect to `localhost:9092` inside the container — which fails.

**Fix (set on the Kafka/Redpanda side):**

| Setup | Config |
|---|---|
| Bare Kafka `server.properties` | `advertised.listeners=PLAINTEXT://host.docker.internal:9092` |
| Docker Compose — Confluent image | `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://host.docker.internal:9092` |
| Docker Compose — Bitnami image | `KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://host.docker.internal:9092` |
| Redpanda Docker | `--advertise-kafka-addr host.docker.internal:9092` |

This change does not break connections from outside Docker — `host.docker.internal`
resolves to `127.0.0.1` on the host itself.

---

## 8. Implementation steps

### ✅ Step 1 — Java manual prediction consumer

1. ✅ Added `src/main/java/com/cs2predictor/inference/web/ManualRequestConsumer.java`
   - Plain `KafkaConsumer` polling `cs2.gsi.manual.requests`
   - On each message: builds one-row `List<Row>` → `spark.newSession().createDataFrame()`
     → `model.transform()` → extracts `probability[1]`
   - Publishes `{"correlationId": ..., "probCtWin": ...}` to `cs2.gsi.manual.responses`
   - On failure: publishes `{"correlationId": ..., "error": "..."}` so the UI doesn't hang
2. ✅ Updated `Cs2InferenceApplication.java` — starts `ManualRequestConsumer` in a named
   daemon thread after model load, before `query.awaitTermination()`

### ✅ Step 2 — Node.js server (done 2026-05-18)

1. ✅ Created `cs2-predictor-ui/package.json` with `express ^4.19.0` and `kafkajs ^2.2.4`
2. ✅ Ran `npm install` — 69 packages, 0 vulnerabilities
3. ✅ Wrote `cs2-predictor-ui/server.js`:
   - Top-of-file config via env vars (`WEB_PORT`, `KAFKA_BROKERS`, `KAFKA_TOPIC`,
     `MANUAL_REQUEST_TOPIC`, `MANUAL_RESPONSE_TOPIC`)
   - In-memory `allPredictions[]` and `roundHistory` Map (per-session → per-round ticks)
   - KafkaJS consumer for live predictions (`fromBeginning: true`, group `cs2-predictor-ui`)
   - KafkaJS consumer for manual responses (`fromBeginning: false`, group `cs2-predictor-ui-responses`)
   - KafkaJS producer for manual requests
   - All Kafka connections are best-effort; server starts even if Kafka is down
   - `GET /api/history` — full snapshot JSON
   - `GET /api/events` — SSE with 30-second heartbeat to survive proxy idle timeouts
   - `POST /api/predict` — publishes to request topic, awaits correlated response (10 s timeout)
   - `express.static` serves `public/`
4. ✅ Created `cs2-predictor-ui/public/` (placeholder; frontend files added in Step 3)

### ✅ Step 3 — Frontend (done 2026-05-18)

1. ✅ Wrote `public/index.html`:
   - Bootstrap 5.3, Chart.js 4.4.0, chartjs-adapter-date-fns 3, Hammer.js 2, chartjs-plugin-zoom 2.0.1 (all CDN)
   - Navbar with live status dot; latest-prediction card + main chart row; per-round accordion; manual prediction form
   - "Reset zoom" button on main chart; "Pre-fill from live" resets to balanced round-start defaults

2. ✅ Wrote `public/app.js`:
   - `fetch('/api/history')` on load seeds `roundData`, main chart datasets, and accordion items; expands most recent round
   - `new EventSource('/api/events')` appends live ticks; expands accordion item on first tick of a new round
   - One Chart.js dataset per `sessionKey` in the main time-series chart (x = epoch ms `time` scale, wheel/pinch zoom)
   - Per-round mini charts (x = tick index 1…N, y = probCtWin); `resize()` called on `shown.bs.collapse` to fix hidden-canvas sizing
   - Latest panel colour-coded green/red on CT/T lead; border updates on each tick
   - Form submit `POST /api/predict`, shows spinner, renders result progress bar

3. ✅ Wrote `public/style.css` — live-dot pulse animation, chart container heights (290 px main, 120 px mini), accordion font size

### ✅ Step 4 — Wire together and smoke test (done 2026-05-18)

#### Bug found and fixed: live predictions consumer stalled on Redpanda Serverless

After moving the broker to Redpanda Serverless, the live predictions consumer silently stalled
while manual predictions continued to work. Root cause: `fromBeginning: true` causes KafkaJS to
seek to the earliest offset; on Redpanda Serverless this leaves the consumer stalled when the
consumer group has stale committed offsets. Fix: changed `fromBeginning: true` → `fromBeginning: false`
for the live predictions consumer, and added cloud-appropriate Kafka instance settings
(`connectionTimeout: 3000`, `requestTimeout: 30000`, `retry: { retries: 5 }`) and consumer
settings (`sessionTimeout: 30000`, `heartbeatInterval: 3000`).

#### Bug found and fixed: timestamp in epoch-seconds instead of epoch-milliseconds

Spark's `TimestampType → LongType` cast produces **epoch-seconds**, but Chart.js `time` scale
requires **epoch-milliseconds** and the design contract specifies ms.  Two fixes applied:

- `PredictionSink.java` — changed `col("event_timestamp")` to `col("event_timestamp").multiply(1000L)`
  so all new Kafka messages carry epoch-ms timestamps.
- `app.js` — added `toMs(ts)` helper (`ts < 1e12 ? ts * 1000 : ts`) applied when pushing points to
  the main Chart.js dataset, so existing retained Kafka messages (seconds) also render correctly.

#### Other pre-flight fixes

- `.gitignore` — added `cs2-predictor-ui/node_modules/`
- `server.js` — added null guard for Kafka tombstone messages (`if (!message.value) return`)

#### Endpoint smoke test results (all pass)

| Endpoint | Result |
|---|---|
| `POST http://localhost:3000/api/predict` (Kafka round-trip) | `{"probCtWin":0.7564}` ✅ |
| `GET http://localhost:3000/api/history` | 11 808 predictions, rounds 0–23 ✅ |
| `GET http://localhost:3000/api/events` | HTTP 200, `text/event-stream` ✅ |
| `GET http://localhost:3000/` | HTTP 200, `index.html` (7 005 bytes) ✅ |

### ✅ Step 5 — Docker image (done 2026-05-18)

1. ✅ Created `cs2-predictor-ui/Dockerfile` — `node:22-alpine`, `npm ci --omit=dev`, exposes 3000
2. ✅ Created `cs2-predictor-ui/.dockerignore` — excludes `node_modules`
3. ✅ Verified: `docker build` succeeds; container starts, `GET /` → 200, `GET /api/history` → `{}`

---

## 9. To start the full stack

### Production / Redpanda Serverless (primary setup)

```bash
# 1. Inference app (Spark streaming + ManualRequestConsumer)
#    Credentials are already in config/inference-config.yaml
mvn exec:exec

# 2a. Web UI — local Node.js
WEB_PORT=3000 \
  KAFKA_BROKERS=d85nitghvfrjvm53g2ug.any.eu-central-1.mpx.prd.cloud.redpanda.com:9092 \
  KAFKA_SASL_MECHANISM=scram-sha-256 \
  KAFKA_SASL_USERNAME=cs2-brocker \
  KAFKA_SASL_PASSWORD=sKUVi0gQajnd2rcPrUGacQ5A5MX8Wa \
  node cs2-predictor-ui/server.js

# 2b. Web UI — Docker
docker build -t cs2-predictor-ui ./cs2-predictor-ui
docker run -p 3000:3000 --dns 8.8.8.8 \
  -e KAFKA_BROKERS=d85nitghvfrjvm53g2ug.any.eu-central-1.mpx.prd.cloud.redpanda.com:9092 \
  -e KAFKA_SASL_MECHANISM=scram-sha-256 \
  -e KAFKA_SASL_USERNAME=cs2-brocker \
  -e KAFKA_SASL_PASSWORD=sKUVi0gQajnd2rcPrUGacQ5A5MX8Wa \
  -e CONSUMER_GROUP=cs2-predictor-ui-2 \
  cs2-predictor-ui
# --dns 8.8.8.8 required: Docker's internal DNS cannot resolve the per-broker
# hostnames Redpanda Serverless advertises in its metadata response.
# CONSUMER_GROUP: use a fresh group name if the stream shows nothing (see ghost consumer note, Section 11).

# 3. Open http://localhost:3000
```

### Local / dev setup (backup)

```bash
# 1. Start local Redpanda in Docker
docker start redpanda-cs2
# (first run: see Redpanda quick-start command in GSI_BRIDGE_DESIGN.md)

# 2. Inference app
mvn exec:exec

# 3a. Web UI — local Node.js (no SASL needed for local broker)
WEB_PORT=3000 KAFKA_BROKERS=localhost:9092 node cs2-predictor-ui/server.js

# 3b. Web UI — Docker (local broker)
docker build -t cs2-predictor-ui ./cs2-predictor-ui
docker run -p 3000:3000 \
  -e KAFKA_BROKERS=host.docker.internal:9092 \
  cs2-predictor-ui
# NOTE: local broker must advertise host.docker.internal:9092 (see Section 7)

# 4. Open http://localhost:3000
```

---

## 10. Configuration reference

### `cs2-predictor-ui/server.js` env vars

| Variable | Default | Description |
|---|---|---|
| `WEB_PORT` | `3000` | HTTP listen port |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `KAFKA_TOPIC` | `cs2.gsi.predictions` | Live prediction topic (read) |
| `MANUAL_REQUEST_TOPIC` | `cs2.gsi.manual.requests` | Manual prediction requests (write) |
| `MANUAL_RESPONSE_TOPIC` | `cs2.gsi.manual.responses` | Manual prediction responses (read) |
| `KAFKA_SASL_MECHANISM` | _(empty — no SASL)_ | e.g. `scram-sha-256`; enables SSL+SASL when set |
| `KAFKA_SASL_USERNAME` | _(empty)_ | SASL username |
| `KAFKA_SASL_PASSWORD` | _(empty)_ | SASL password |
| `CONSUMER_GROUP` | `cs2-predictor-ui` | Base name for both Kafka consumer groups: predictions stream uses `CONSUMER_GROUP`, manual-prediction responses use `CONSUMER_GROUP-responses`. Change this if either the stream or manual predictions stop working — see the ghost consumer note in Section 11. |
| `CLEAR_KEY` | `cs2clear` | Secret key required by the Clear History button in the UI |

When `KAFKA_SASL_MECHANISM` is set, KafkaJS is configured with `ssl: true` and the
`sasl` block. Leave all three unset for a plaintext local broker.

---

## 11. Notes and constraints

### Memory: `allPredictions` is unbounded
Typical CS2 match: ~30 rounds × ~20 ticks/round × 1 s/tick ≈ 600 messages. At ~200
bytes/message that is ~120 KB per match — negligible. If the server runs for many
matches, add a rolling cap: keep the last N messages and one summary entry per round.

### Thread safety of `spark.newSession()`
`SparkSession.newSession()` shares the underlying `SparkContext` (thread-safe) but
isolates SQL configuration. Batch jobs submitted via `ManualRequestConsumer` queue
behind streaming micro-batches in the local thread pool. Latency for a single-row
transform is <100 ms at `local[4]` — acceptable for an interactive form.

### `fromBeginning: false` in the live predictions consumer
The live consumer uses `fromBeginning: false` (resume from committed offset, or start
from latest for a brand-new consumer group). Using `fromBeginning: true` causes KafkaJS
to seek to the earliest offset on every startup; on Redpanda Serverless this leaves the
consumer silently stalled when the group's previously committed offset is stale or the
internal seek does not complete. The manual-response consumer has always used
`fromBeginning: false` and works reliably — aligning the live consumer to the same
setting fixes the stream.

### No WebSocket needed
Server-Sent Events (SSE) are one-directional and sufficient here. The browser only
reads the prediction stream; it writes only for manual prediction (regular HTTP POST).
SSE reconnects automatically on drop; no library needed in the browser.

### Docker DNS for Redpanda Serverless
Redpanda Serverless returns per-broker hostnames in its metadata response (e.g.
`d85nitghvfrjvm53g2ug-29.1.eu-central-1.mpx.prd.cloud.redpanda.com`). Docker's
default internal DNS resolver cannot resolve these hostnames, causing every message
fetch and produce to fail with `EAI_AGAIN` even though the bootstrap connection
succeeds. Fix: add `--dns 8.8.8.8` to `docker run` so the container uses a resolver
that can reach these advertised broker addresses. The startup commands in Section 9
already include this flag.

### Ghost consumer group blocking the predictions stream

**Symptom:** live stream shows nothing, or manual predictions time out, or both; container logs show `memberAssignment: {}` for the affected consumer group.

**Cause:** Redpanda Serverless keeps dead consumer sessions alive well beyond the client-side `sessionTimeout` (observed: 30+ minutes despite `sessionTimeout: 30_000`). A previous `server.js` process or Docker container that exited without a clean shutdown leaves a "ghost" member in the group. With one partition per topic, the ghost holds it and the new consumer gets nothing.

There are two consumer groups, both derived from `CONSUMER_GROUP`:
- `CONSUMER_GROUP` (default `cs2-predictor-ui`) — live predictions stream
- `CONSUMER_GROUP-responses` (default `cs2-predictor-ui-responses`) — manual prediction responses

A ghost in either group breaks the corresponding feature. Changing `CONSUMER_GROUP` creates fresh names for **both** groups at once, fixing both symptoms.

**Fix:** pass a different value for `CONSUMER_GROUP` when starting the container (e.g. `cs2-predictor-ui-2`). Both consumer groups get fresh names with no ghost members.

```bash
docker run ... -e CONSUMER_GROUP=cs2-predictor-ui-2 cs2-predictor-ui
```

Increment the suffix each time you hit the problem, or use the Redpanda Console to delete the stale consumer group manually.

### Docker + Kafka advertised listener (local broker only)
For a local Kafka/Redpanda broker running in Docker, the broker must advertise
`host.docker.internal:9092` for the containerised UI to reach it. See Section 7.
This does not apply when connecting to Redpanda Serverless.

---

## 12. Updating the Docker image on Docker Hub

Run these three commands from the project root whenever you want to publish a new version:

```bash
# 1. Rebuild the image
docker build -t cs2-predictor-ui ./cs2-predictor-ui

# 2. Tag for Docker Hub
docker tag cs2-predictor-ui:latest sulimaivan/cs2-predictor-ui:latest

# 3. Push
docker push sulimaivan/cs2-predictor-ui:latest
```

The image is public at `docker.io/sulimaivan/cs2-predictor-ui:latest`.

---

## 13. AWS EC2 deployment

### Existing AWS resources (do not recreate)

| Resource | ID / Name | Region |
|---|---|---|
| Stopped EC2 instance | `i-00c40cee63f5dece0` | eu-central-1 |
| Security group | `sg-0a6e45720ba12c19d` (`cs2-predictor-ui-sg`) | eu-central-1 |
| Key pair | `cs2-predictor-key` | eu-central-1 |
| Key file | `cs2-predictor-key.pem` (project root) | — |
| AMI (Docker pre-installed) | `ami-01bc40b4960a79259` (`cs2-predictor-ui-docker-ready`) | eu-central-1 |

Security group rules: inbound TCP 22 (SSH) and TCP 3000 (web UI) from `0.0.0.0/0`.

### Option A — Restart the existing stopped instance

The simplest path. The container is already configured on the instance.

```bash
# Start
aws ec2 start-instances --region eu-central-1 --instance-ids i-00c40cee63f5dece0

# Get new public DNS (changes on every start unless you assign an Elastic IP)
aws ec2 describe-instances --region eu-central-1 --instance-ids i-00c40cee63f5dece0 \
  --query "Reservations[0].Instances[0].PublicDnsName" --output text

# Stop when done
aws ec2 stop-instances --region eu-central-1 --instance-ids i-00c40cee63f5dece0
```

Open `http://<public-dns>:3000` in the browser.

> **Note:** The public DNS changes every time the instance starts. Assign an Elastic IP
> (free while the instance is running) if you need a stable address.

### Option B — Launch a fresh instance from the AMI

Use this if the existing instance is terminated or you need a clean deployment.
The AMI (`ami-01bc40b4960a79259`) already has Docker installed, so the user-data
script only needs to start the container (no package install step).

```bash
# Encode the startup script
USERDATA=$(cat <<'EOF'
#!/bin/bash
systemctl start docker
docker run -d --restart always -p 3000:3000 --dns 8.8.8.8 \
  -e KAFKA_BROKERS=d85nitghvfrjvm53g2ug.any.eu-central-1.mpx.prd.cloud.redpanda.com:9092 \
  -e KAFKA_SASL_MECHANISM=scram-sha-256 \
  -e KAFKA_SASL_USERNAME=cs2-brocker \
  -e KAFKA_SASL_PASSWORD=sKUVi0gQajnd2rcPrUGacQ5A5MX8Wa \
  -e CONSUMER_GROUP=cs2-predictor-ui-2 \
  sulimaivan/cs2-predictor-ui:latest
EOF
)

aws ec2 run-instances \
  --region eu-central-1 \
  --image-id ami-01bc40b4960a79259 \
  --instance-type t3.micro \
  --key-name cs2-predictor-key \
  --security-group-ids sg-0a6e45720ba12c19d \
  --user-data "$USERDATA" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=cs2-predictor-ui}]" \
  --query "Instances[0].InstanceId" --output text
```

Wait ~30 seconds (container starts much faster than a full Docker install), then get the public DNS as in Option A.

### Updating a running instance to the latest image

SSH in and replace the container (`ec2-user` requires `sudo` for Docker):

```bash
ssh -i cs2-predictor-key.pem ec2-user@<public-dns>

sudo docker pull sulimaivan/cs2-predictor-ui:latest
sudo docker stop $(sudo docker ps -q)
sudo docker rm $(sudo docker ps -aq)
sudo docker run -d --restart always -p 3000:3000 --dns 8.8.8.8 \
  -e KAFKA_BROKERS=d85nitghvfrjvm53g2ug.any.eu-central-1.mpx.prd.cloud.redpanda.com:9092 \
  -e KAFKA_SASL_MECHANISM=scram-sha-256 \
  -e KAFKA_SASL_USERNAME=cs2-brocker \
  -e KAFKA_SASL_PASSWORD=sKUVi0gQajnd2rcPrUGacQ5A5MX8Wa \
  -e CONSUMER_GROUP=cs2-predictor-ui-2 \
  sulimaivan/cs2-predictor-ui:latest
```

Or as a one-liner from your local machine (no interactive SSH needed):

```bash
ssh -i cs2-predictor-key.pem -o StrictHostKeyChecking=no ec2-user@<public-dns> \
  "sudo docker pull sulimaivan/cs2-predictor-ui:latest && \
   sudo docker stop \$(sudo docker ps -q) 2>/dev/null; sudo docker rm \$(sudo docker ps -aq) 2>/dev/null; \
   sudo docker run -d --restart always -p 3000:3000 --dns 8.8.8.8 \
     -e KAFKA_BROKERS=d85nitghvfrjvm53g2ug.any.eu-central-1.mpx.prd.cloud.redpanda.com:9092 \
     -e KAFKA_SASL_MECHANISM=scram-sha-256 \
     -e KAFKA_SASL_USERNAME=cs2-brocker \
     -e KAFKA_SASL_PASSWORD=sKUVi0gQajnd2rcPrUGacQ5A5MX8Wa \
     -e CONSUMER_GROUP=cs2-predictor-ui-2 \
     sulimaivan/cs2-predictor-ui:latest"
```
