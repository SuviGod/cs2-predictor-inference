'use strict';

const express        = require('express');
const path           = require('path');
const { randomUUID } = require('crypto');
const { Kafka }      = require('kafkajs');

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const WEB_PORT              = process.env.WEB_PORT              || 3000;
const KAFKA_BROKERS         = process.env.KAFKA_BROKERS         || 'localhost:9092';
const KAFKA_TOPIC           = process.env.KAFKA_TOPIC           || 'cs2.gsi.predictions';
const MANUAL_REQUEST_TOPIC  = process.env.MANUAL_REQUEST_TOPIC  || 'cs2.gsi.manual.requests';
const MANUAL_RESPONSE_TOPIC = process.env.MANUAL_RESPONSE_TOPIC || 'cs2.gsi.manual.responses';
const KAFKA_SASL_MECHANISM  = process.env.KAFKA_SASL_MECHANISM  || '';
const KAFKA_SASL_USERNAME   = process.env.KAFKA_SASL_USERNAME   || '';
const KAFKA_SASL_PASSWORD   = process.env.KAFKA_SASL_PASSWORD   || '';
const CLEAR_KEY             = process.env.CLEAR_KEY             || 'cs2clear';
const CONSUMER_GROUP        = process.env.CONSUMER_GROUP        || 'cs2-predictor-ui';
const PREDICT_TIMEOUT_MS    = 10_000;

// ---------------------------------------------------------------------------
// In-memory store
// ---------------------------------------------------------------------------

// All predictions received this server session, in arrival order.
// Shape: [{ sessionKey, round, probCtWin, timestamp }, ...]
const allPredictions = [];

// Per-session, per-round tick arrays.
// Shape: Map<sessionKey → Map<roundNumber → [{ probCtWin, timestamp }, ...]>>
const roundHistory = new Map();

function storePrediction(msg) {
  allPredictions.push(msg);

  if (!roundHistory.has(msg.sessionKey)) {
    roundHistory.set(msg.sessionKey, new Map());
  }
  const sessionMap = roundHistory.get(msg.sessionKey);
  if (!sessionMap.has(msg.round)) {
    sessionMap.set(msg.round, []);
  }
  sessionMap.get(msg.round).push({ probCtWin: msg.probCtWin, timestamp: msg.timestamp });
}

// Serialize roundHistory to a plain object suitable for JSON.stringify.
function roundHistoryToObject() {
  const result = {};
  for (const [sessionKey, rounds] of roundHistory) {
    result[sessionKey] = {};
    for (const [round, ticks] of rounds) {
      result[sessionKey][round] = ticks;
    }
  }
  return result;
}

// ---------------------------------------------------------------------------
// SSE client registry
// ---------------------------------------------------------------------------
const sseClients = new Set();

function broadcastSSE(msg) {
  const data = `data: ${JSON.stringify(msg)}\n\n`;
  for (const res of sseClients) {
    res.write(data);
  }
}

// ---------------------------------------------------------------------------
// Pending manual prediction requests
// correlationId → { resolve, reject, timeoutId }
// ---------------------------------------------------------------------------
const pendingRequests = new Map();

// ---------------------------------------------------------------------------
// Kafka
// ---------------------------------------------------------------------------
let kafkaProducer = null;

async function startKafka() {
  const kafkaConfig = {
    brokers: KAFKA_BROKERS.split(','),
    clientId: 'cs2-ui',
    connectionTimeout: 3_000,
    requestTimeout:    30_000,
    retry: { retries: 5 },
  };
  if (KAFKA_SASL_MECHANISM) {
    kafkaConfig.ssl = true;
    kafkaConfig.sasl = {
      mechanism: KAFKA_SASL_MECHANISM,
      username:  KAFKA_SASL_USERNAME,
      password:  KAFKA_SASL_PASSWORD,
    };
  }
  const kafka = new Kafka(kafkaConfig);

  const CONSUMER_GROUP_CONFIG = { sessionTimeout: 30_000, heartbeatInterval: 3_000 };

  // Consumer: live predictions.
  // fromBeginning: false — resume from committed offset, or start from latest on a
  // fresh group. Using true causes KafkaJS to seek to the earliest offset; on cloud
  // brokers (Redpanda Serverless) this can leave the consumer silently stalled when
  // the group's committed offset is stale or the seek fails internally.
  const consumer = kafka.consumer({ groupId: CONSUMER_GROUP, ...CONSUMER_GROUP_CONFIG });
  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  consumer.on(consumer.events.CRASH, ({ payload }) => {
    console.error('Live predictions consumer crashed — will restart:', payload.error.message);
  });
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        if (!message.value) return;                          // skip tombstones
        const msg = JSON.parse(message.value.toString());
        storePrediction(msg);
        broadcastSSE(msg);
      } catch (err) {
        console.error('Failed to parse prediction message:', err.message);
      }
    },
  });

  // Consumer: manual prediction responses (latest only — responses are transient)
  const responseConsumer = kafka.consumer({ groupId: CONSUMER_GROUP + '-responses', ...CONSUMER_GROUP_CONFIG });
  await responseConsumer.connect();
  await responseConsumer.subscribe({ topic: MANUAL_RESPONSE_TOPIC, fromBeginning: false });
  await responseConsumer.run({
    eachMessage: async ({ message }) => {
      try {
        if (!message.value) return;
        const msg = JSON.parse(message.value.toString());
        const pending = pendingRequests.get(msg.correlationId);
        if (!pending) return;                               // response for a timed-out request
        clearTimeout(pending.timeoutId);
        pendingRequests.delete(msg.correlationId);
        if (msg.error) {
          pending.reject(new Error(msg.error));
        } else {
          pending.resolve({ probCtWin: msg.probCtWin });
        }
      } catch (err) {
        console.error('Failed to process manual response:', err.message);
      }
    },
  });

  // Producer: manual prediction requests
  const producer = kafka.producer();
  await producer.connect();
  kafkaProducer = producer;

  console.log(`Kafka connected — predictions: ${KAFKA_TOPIC}`);
  console.log(`                  manual: ${MANUAL_REQUEST_TOPIC} → ${MANUAL_RESPONSE_TOPIC}`);
}

// ---------------------------------------------------------------------------
// Express app
// ---------------------------------------------------------------------------
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// GET / — serve index.html (handled by express.static above; explicit fallback)
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// GET /api/history — full snapshot of stored predictions + round history
app.get('/api/history', (req, res) => {
  res.json({
    predictions: allPredictions,
    roundHistory: roundHistoryToObject(),
  });
});

// GET /api/events — SSE stream
app.get('/api/events', (req, res) => {
  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.flushHeaders();

  // Send a comment heartbeat every 30 s to keep the connection alive through
  // proxies that close idle connections.
  const heartbeat = setInterval(() => res.write(': heartbeat\n\n'), 30_000);

  sseClients.add(res);

  req.on('close', () => {
    clearInterval(heartbeat);
    sseClients.delete(res);
  });
});

// DELETE /api/history — clear in-memory store; requires secret key in x-clear-key header
app.delete('/api/history', (req, res) => {
  if ((req.headers['x-clear-key'] || '') !== CLEAR_KEY) {
    return res.status(401).json({ error: 'Invalid key' });
  }
  allPredictions.length = 0;
  roundHistory.clear();
  broadcastSSE({ type: 'clear' });
  res.json({ ok: true });
});

// POST /api/predict — publish request to Kafka, wait for correlated response
app.post('/api/predict', async (req, res) => {
  if (!kafkaProducer) {
    return res.status(503).json({ error: 'Kafka not connected — manual predictions unavailable' });
  }

  const correlationId = randomUUID();

  const promise = new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      pendingRequests.delete(correlationId);
      reject(new Error('Prediction timed out — inference app may be down'));
    }, PREDICT_TIMEOUT_MS);
    pendingRequests.set(correlationId, { resolve, reject, timeoutId });
  });

  try {
    await kafkaProducer.send({
      topic: MANUAL_REQUEST_TOPIC,
      messages: [{ key: correlationId, value: JSON.stringify({ correlationId, ...req.body }) }],
    });
    const result = await promise;
    res.json(result);
  } catch (err) {
    pendingRequests.delete(correlationId);
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------
async function main() {
  // Kafka connection is best-effort; if the broker is not running the server
  // still starts and serves HTTP (manual predictions will return 503).
  startKafka().catch((err) => {
    console.warn('Kafka failed to connect (live updates and manual predictions unavailable):', err.message);
  });

  app.listen(WEB_PORT, () => {
    console.log(`cs2-predictor-ui listening on http://localhost:${WEB_PORT}`);
  });
}

main();
