# CS2 Predictor — Web UI Design

> **Purpose:** Design for a real-time browser dashboard that reads predictions from
> `cs2.gsi.predictions`, displays live probability, time-series charts, per-round
> history, and lets users manually submit a feature set for an ad-hoc prediction.

---

## Progress

| Step | Component | Status |
|---|---|---|
| 1 | Java REST endpoint (`ManualPredictionServer`) | ✅ Done |
| 2 | Node.js web server (`cs2-predictor-ui/server.js`) | ⬜ Pending |
| 3 | Frontend (`index.html`, `app.js`, `style.css`) | ⬜ Pending |
| 4 | End-to-end wiring and smoke test | ⬜ Pending |

---

## 1. Architecture overview

```
cs2.gsi.predictions (Kafka/Redpanda)
        │
        ▼
cs2-predictor-ui/server.js          ← Node.js + Express + kafkajs
        │  in-memory store (history + per-round map)
        ├── GET /                   → serves index.html
        ├── GET /api/history        → full history snapshot (JSON)
        ├── GET /api/events         → SSE stream (live updates)
        └── POST /api/predict       → proxy to Java REST API
                                            │
                                            ▼
                                   Java ManualPredictionServer  (port 7070)  ✅
                                   POST /api/predict
                                            │
                                            ▼
                                   spark.newSession()
                                   + PipelineModel.transform()
                                            │
                                            ▼
                                   { "probCtWin": 0.73 }

Browser ←── SSE (live ticks) + HTTP (history, manual)
```

Two new components are added:

| Component | Location | New dependency | Status |
|---|---|---|---|
| Java REST endpoint | Inline in `cs2-predictor-inference` | None (JDK `com.sun.net.httpserver`) | ✅ Done |
| Node.js web server | New directory `cs2-predictor-ui/` | `express`, `kafkajs` | ⬜ Pending |
| Frontend | `cs2-predictor-ui/public/` | Chart.js, Bootstrap 5 (CDN) | ⬜ Pending |

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

## 4. Java REST endpoint (`ManualPredictionServer`) — ✅ DONE

### What was built

| Item | Detail |
|---|---|
| File | `src/main/java/com/cs2predictor/inference/web/ManualPredictionServer.java` |
| Config class | `src/main/java/com/cs2predictor/inference/config/WebConfig.java` |
| Config key | `web.port` in `inference-config.yaml` (default `7070`) |
| New Maven deps | None — uses JDK's `com.sun.net.httpserver` + existing `jackson-databind` |
| Thread model | Daemon thread with a 2-thread `Executors.newFixedThreadPool` executor |

### Endpoint

```
POST http://localhost:7070/api/predict
Content-Type: application/json
```

**Request body:**
```json
{
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

**Response body:**
```json
{ "probCtWin": 0.7564 }
```

**Error response (HTTP 500):**
```json
{ "error": "reason string" }
```

### Implementation notes

- `spark.newSession()` shares the underlying `SparkContext` with the streaming query
  but has isolated SQL configuration — safe for concurrent batch requests.
- `FEATURE_SCHEMA` column names exactly match the `VectorAssembler`'s `inputCols` in
  the trained `PipelineModel`. Column order in the schema is irrelevant; the assembler
  selects by name.
- CORS headers (`Access-Control-Allow-Origin: *`) and `OPTIONS` preflight handling are
  included so the browser can call port 7070 directly from a page served on port 3000.
- `InferenceConfig` initialises `WebConfig` to its default value (`port=7070`) so the
  `web:` block is optional in the YAML — the server always starts.
- The server is launched in `Cs2InferenceApplication.main()` after `ModelLoader.load()`
  and before `query.awaitTermination()`.

### Verified

```
POST http://localhost:7070/api/predict  (tested 2026-05-15)
→ 200 OK  { "probCtWin": 0.7564238741381034 }
```

---

## 5. Node.js web server — ⬜ PENDING

### Directory layout
```
cs2-predictor-ui/
├── package.json
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

### Kafka consumer (inside `server.js`)

```js
const kafka = new Kafka({ brokers: ['localhost:9092'], clientId: 'cs2-ui' });
const consumer = kafka.consumer({ groupId: 'cs2-predictor-ui' });

// fromBeginning: true — replays retained messages on startup so the chart
// populates immediately without waiting for live events.
await consumer.subscribe({ topic: 'cs2.gsi.predictions', fromBeginning: true });
await consumer.run({ eachMessage: ({ message }) => {
  const msg = JSON.parse(message.value.toString());
  allPredictions.push(msg);
  // append to roundHistory[sessionKey][round]
  broadcastSSE(msg);
}});
```

### HTTP endpoints (`server.js`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Serves `public/index.html` |
| `GET` | `/api/history` | Returns `{ predictions: allPredictions, roundHistory }` as JSON |
| `GET` | `/api/events` | SSE stream; sends `data: <json>\n\n` per new prediction |
| `POST` | `/api/predict` | Forwards body to `http://localhost:7070/api/predict`, returns result |

### Configuration (top of `server.js`)
```js
const WEB_PORT         = process.env.WEB_PORT         || 3000;
const KAFKA_BROKERS    = process.env.KAFKA_BROKERS    || 'localhost:9092';
const JAVA_PREDICT_URL = process.env.JAVA_PREDICT_URL || 'http://localhost:7070/api/predict';
```

---

## 6. Frontend (`public/index.html` + `public/app.js`) — ⬜ PENDING

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

#### 6.4 Manual prediction form
- Ten labelled inputs (number fields with sensible min/max/step):
  `CT alive` (0–5), `T alive` (0–5), `CT total HP` (0–500),
  `T total HP` (0–500), `Bomb planted` (checkbox → 0/1),
  `Time remaining` (0–115, step 0.1),
  `CT kills prev3` (0–), `CT damage prev3` (0–),
  `T kills prev3` (0–), `T damage prev3` (0–).
- Submit button calls `POST /api/predict` on the Node.js server (which proxies to
  Java). Shows spinner while waiting.
- Result displayed below the form: probability bar + percentage.
- Pre-fill button: copies the values from the latest live prediction into the form
  so the user can tweak a real game state.

---

## 7. Implementation steps

### ✅ Step 1 — Java REST endpoint

1. ✅ Added `src/main/java/com/cs2predictor/inference/config/WebConfig.java`
2. ✅ Added `web` field to `InferenceConfig.java` (default-initialised — YAML block optional)
3. ✅ Added `web.port: 7070` to `config/inference-config.yaml`
4. ✅ Added `src/main/java/com/cs2predictor/inference/web/ManualPredictionServer.java`
   - `HttpServer.create(new InetSocketAddress(port), 0)`
   - Single `HttpContext` at `/api/predict`
   - Parses request JSON with `ObjectMapper`
   - Builds one-row `List<Row>` → `spark.newSession().createDataFrame()` → `model.transform()`
   - Extracts `vector_to_array(col("probability"), "float64").getItem(1)`
   - Returns `{"probCtWin": <double>}`; `{"error": "..."}` on failure
   - CORS + OPTIONS preflight handled
5. ✅ Updated `Cs2InferenceApplication.java` — starts `ManualPredictionServer` in a
   named daemon thread after model load, before `query.awaitTermination()`

### ⬜ Step 2 — Node.js server

1. `mkdir cs2-predictor-ui && cd cs2-predictor-ui`
2. `npm init -y && npm install express kafkajs`
3. Write `server.js` (Kafka consumer + Express routes + SSE broadcaster)
4. Create `public/` directory

### ⬜ Step 3 — Frontend

1. Write `public/index.html`:
   - Bootstrap 5 CDN, Chart.js 4 CDN, chartjs-plugin-zoom CDN
   - Four sections: header, latest-panel + main-chart row, accordion, manual form
   - One `<script src="app.js">` tag

2. Write `public/app.js`:
   - On load: `fetch('/api/history')` → seed charts and accordion
   - `new EventSource('/api/events')` → live updates without page refresh
   - Form submit: `fetch('/api/predict', {method:'POST', body:JSON.stringify(...)})`

3. Write `public/style.css` (minimal, on top of Bootstrap)

### ⬜ Step 4 — Wire together and smoke test

1. Start Redpanda: `docker start redpanda-cs2`
2. Start inference app: `mvn exec:exec` (REST API auto-starts on port 7070)
3. Start UI server: `node cs2-predictor-ui/server.js`
4. Open `http://localhost:3000`
5. Verify SSE updates arrive when GSI events flow through the pipeline
6. Verify manual prediction form returns a result

---

## 8. Configuration reference

### `inference-config.yaml` additions (already applied)
```yaml
web:
  port: 7070
```

### `cs2-predictor-ui/server.js` env vars (Step 2)
```
WEB_PORT=3000
KAFKA_BROKERS=localhost:9092
JAVA_PREDICT_URL=http://localhost:7070/api/predict
```

---

## 9. Notes and constraints

### Memory: `allPredictions` is unbounded
Typical CS2 match: ~30 rounds × ~20 ticks/round × 1 s/tick ≈ 600 messages. At ~200
bytes/message that is ~120 KB per match — negligible. If the server runs for many
matches, add a rolling cap: keep the last N messages and one summary entry per round.

### Thread safety of `spark.newSession()`
`SparkSession.newSession()` shares the underlying `SparkContext` (thread-safe) but
isolates SQL configuration. Batch jobs submitted via the REST endpoint queue behind
streaming micro-batches in the local thread pool. Latency for a single-row transform
is <100 ms at `local[4]` — acceptable for an interactive form.

### `fromBeginning: true` in the Kafka consumer
Redpanda defaults to 7-day retention. The UI server replays all retained messages on
startup so charts populate immediately. If retention is too large, switch to
`fromBeginning: false` and show only live updates.

### CORS
`ManualPredictionServer` sets `Access-Control-Allow-Origin: *` and handles `OPTIONS`
preflight so the browser can call `localhost:7070` from a page served on `localhost:3000`.
Already implemented in Step 1.

### No WebSocket needed
Server-Sent Events (SSE) are one-directional and sufficient here. The browser only
reads the prediction stream; it writes only for manual prediction (regular HTTP POST).
SSE reconnects automatically on drop; no library needed in the browser.
