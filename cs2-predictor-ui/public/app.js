'use strict';

// ── State ─────────────────────────────────────────────────────────────────────
let latestPrediction     = null;
let mainChart            = null;
const sessionColors      = {};
let nextColorIdx         = 0;
let formSubmitAttempted  = false;

// roundData[roundNum] = [{ probCtWin, timestamp }, ...]  (aggregated across sessions)
const roundData   = {};
// roundCharts[roundNum] = Chart instance for that accordion item
const roundCharts = {};

const COLOR_PALETTE = [
  '#4e79a7','#f28e2b','#e15759','#76b7b2',
  '#59a14f','#edc948','#b07aa1','#ff9da7',
];

// ── Helpers ───────────────────────────────────────────────────────────────────
function colorForSession(key) {
  if (!sessionColors[key]) {
    sessionColors[key] = COLOR_PALETTE[nextColorIdx % COLOR_PALETTE.length];
    nextColorIdx++;
  }
  return sessionColors[key];
}

// Spark's TimestampType → LongType cast produces epoch-seconds; Chart.js needs epoch-ms.
// PredictionSink now multiplies by 1000, but retained Kafka messages may still be in seconds.
function toMs(ts) {
  return ts < 1e12 ? ts * 1000 : ts;
}

function hexToRgba(hex, alpha) {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

// ── Main chart ────────────────────────────────────────────────────────────────
function initMainChart() {
  const ctx = document.getElementById('mainChart').getContext('2d');
  mainChart = new Chart(ctx, {
    type: 'line',
    data: { datasets: [] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      scales: {
        x: {
          type: 'time',
          time: { tooltipFormat: 'HH:mm:ss' },
          title: { display: true, text: 'Time' },
        },
        y: {
          min: 0,
          max: 1,
          title: { display: true, text: 'CT Win Prob' },
          ticks: { callback: v => Math.round(v * 100) + '%' },
        },
      },
      plugins: {
        legend: { display: true },
        zoom: {
          zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'x' },
          pan:  { enabled: true, mode: 'x' },
        },
      },
    },
  });

  document.getElementById('resetZoomBtn').addEventListener('click', () => {
    mainChart.resetZoom();
  });
}

function addToMainChart(pred) {
  const color = colorForSession(pred.sessionKey);
  const label = '…' + pred.sessionKey.slice(-8);
  let ds = mainChart.data.datasets.find(d => d._sessionKey === pred.sessionKey);
  if (!ds) {
    ds = {
      _sessionKey: pred.sessionKey,
      label,
      borderColor: color,
      backgroundColor: hexToRgba(color, 0.1),
      tension: 0.2,
      pointRadius: 2,
      fill: false,
      data: [],
    };
    mainChart.data.datasets.push(ds);
  }
  ds.data.push({ x: toMs(pred.timestamp), y: pred.probCtWin });
  mainChart.update('none');
}

// ── Latest prediction panel ───────────────────────────────────────────────────
function updateLatestPanel(pred) {
  const pct      = Math.round(pred.probCtWin * 100);
  const isCtWin  = pred.probCtWin >= 0.5;

  document.getElementById('lp-pct').textContent     = pct + '%';
  document.getElementById('lp-side').textContent    = isCtWin ? 'CT Win' : 'T Win';
  document.getElementById('lp-round').textContent   = pred.round;
  document.getElementById('lp-session').textContent = '…' + pred.sessionKey.slice(-8);

  const bar = document.getElementById('lp-bar');
  bar.style.width = pct + '%';
  bar.className   = `progress-bar ${isCtWin ? 'bg-success' : 'bg-danger'}`;

  const panel = document.getElementById('latest-panel');
  panel.className = `card h-100 border-2 ${isCtWin ? 'border-success' : 'border-danger'}`;
}

// ── Live status dot ───────────────────────────────────────────────────────────
function setLiveStatus(connected) {
  document.getElementById('live-dot').className =
    `live-dot ${connected ? 'live-dot--on' : 'live-dot--off'}`;
  const label = document.getElementById('live-label');
  label.textContent = connected ? 'LIVE' : 'DISCONNECTED';
  label.className = `small fw-semibold ${connected ? 'text-success' : 'text-secondary'}`;
}

// ── Per-round accordion ───────────────────────────────────────────────────────
function ensureAccordionItem(round) {
  const id = 'round-' + round;
  if (document.getElementById('acc-item-' + id)) return;

  document.getElementById('noRoundsMsg').classList.add('d-none');

  const accordion = document.getElementById('roundAccordion');
  const item      = document.createElement('div');
  item.className  = 'accordion-item';
  item.id         = 'acc-item-' + id;
  item.innerHTML  = `
    <h2 class="accordion-header">
      <button class="accordion-button collapsed py-2 small" type="button"
              data-bs-toggle="collapse" data-bs-target="#acc-body-${id}">
        <span class="fw-bold me-2">Round ${round}</span>
        <span class="text-success me-1">CT:&nbsp;<span id="acc-ct-${id}">—</span></span>
        <span class="text-danger me-2">T:&nbsp;<span id="acc-t-${id}">—</span></span>
        <span class="badge bg-secondary ms-1" id="acc-ticks-${id}">0 ticks</span>
      </button>
    </h2>
    <div id="acc-body-${id}" class="accordion-collapse collapse">
      <div class="accordion-body p-2">
        <div class="mini-chart-wrap">
          <canvas id="acc-chart-${id}"></canvas>
        </div>
      </div>
    </div>`;
  accordion.appendChild(item);

  const ctx = document.getElementById('acc-chart-' + id).getContext('2d');
  roundCharts[round] = new Chart(ctx, {
    type: 'line',
    data: {
      datasets: [{
        label: 'CT Win Prob',
        borderColor: '#4e79a7',
        backgroundColor: 'rgba(78,121,167,0.15)',
        tension: 0.2,
        pointRadius: 3,
        fill: true,
        data: [],
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      scales: {
        x: {
          type: 'linear',
          title: { display: true, text: 'Tick #' },
          ticks: { precision: 0 },
        },
        y: {
          min: 0,
          max: 1,
          title: { display: true, text: 'Prob' },
          ticks: { callback: v => Math.round(v * 100) + '%' },
        },
      },
      plugins: { legend: { display: false } },
    },
  });

  // Chart.js can't measure a hidden canvas; resize once accordion opens
  document.getElementById('acc-body-' + id).addEventListener('shown.bs.collapse', () => {
    roundCharts[round].resize();
  });
}

function updateAccordionItem(round, expand) {
  const id   = 'round-' + round;
  const ticks = roundData[round];
  const last  = ticks[ticks.length - 1];
  const ctPct = Math.round(last.probCtWin * 100);

  document.getElementById('acc-ct-' + id).textContent    = ctPct + '%';
  document.getElementById('acc-t-' + id).textContent     = (100 - ctPct) + '%';
  document.getElementById('acc-ticks-' + id).textContent =
    ticks.length + (ticks.length === 1 ? ' tick' : ' ticks');

  roundCharts[round].data.datasets[0].data =
    ticks.map((t, i) => ({ x: i + 1, y: t.probCtWin }));
  roundCharts[round].update('none');

  if (expand) {
    const bodyEl = document.getElementById('acc-body-' + id);
    if (!bodyEl.classList.contains('show')) {
      document.querySelectorAll('#roundAccordion .accordion-collapse.show').forEach(el => {
        bootstrap.Collapse.getOrCreateInstance(el, { toggle: false }).hide();
      });
      bootstrap.Collapse.getOrCreateInstance(bodyEl, { toggle: false }).show();
    }
  }
}

// ── Clear history ─────────────────────────────────────────────────────────────
function clearUI() {
  latestPrediction = null;
  Object.keys(roundData).forEach(k => delete roundData[k]);
  Object.keys(roundCharts).forEach(k => { roundCharts[k].destroy(); delete roundCharts[k]; });
  Object.keys(sessionColors).forEach(k => delete sessionColors[k]);
  nextColorIdx = 0;

  mainChart.data.datasets = [];
  mainChart.update('none');

  document.getElementById('lp-pct').textContent     = '—';
  document.getElementById('lp-side').textContent    = 'CT Win';
  document.getElementById('lp-round').textContent   = '—';
  document.getElementById('lp-session').textContent = '—';
  const bar = document.getElementById('lp-bar');
  bar.style.width = '0%';
  bar.className   = 'progress-bar bg-secondary';
  document.getElementById('latest-panel').className = 'card h-100';

  document.getElementById('roundAccordion').innerHTML = '';
  document.getElementById('noRoundsMsg').classList.remove('d-none');
}

document.getElementById('clearHistoryBtn').addEventListener('click', async () => {
  const key = prompt('Enter the clear-history key:');
  if (key === null) return;                                    // user cancelled
  const resp = await fetch('/api/history', {
    method: 'DELETE',
    headers: { 'x-clear-key': key },
  });
  if (resp.status === 401) {
    alert('Wrong key.');
  }
  // On success the server broadcasts a 'clear' SSE event, which triggers clearUI()
  // for this tab and all others.
});

// ── Core: apply one prediction ────────────────────────────────────────────────
function applyPrediction(pred, isLive) {
  updateLatestPanel(pred);
  latestPrediction = pred;
  addToMainChart(pred);

  const isNewRound = !roundData[pred.round];
  if (!roundData[pred.round]) roundData[pred.round] = [];
  roundData[pred.round].push({ probCtWin: pred.probCtWin, timestamp: pred.timestamp });

  ensureAccordionItem(pred.round);
  // During live streaming expand newly-seen rounds; seed pass opens last round after full replay
  updateAccordionItem(pred.round, isLive && isNewRound);
}

// ── Seed from history snapshot ────────────────────────────────────────────────
async function seedHistory() {
  try {
    const resp = await fetch('/api/history');
    const data = await resp.json();
    for (const pred of (data.predictions || [])) {
      applyPrediction(pred, false);
    }
  } catch (err) {
    console.warn('Could not load history:', err.message);
  }
  // Open the most recent round after replay
  if (latestPrediction) {
    const bodyEl = document.getElementById('acc-body-round-' + latestPrediction.round);
    if (bodyEl) bootstrap.Collapse.getOrCreateInstance(bodyEl, { toggle: false }).show();
  }
}

// ── SSE live stream ───────────────────────────────────────────────────────────
function connectSSE() {
  const es    = new EventSource('/api/events');
  es.onopen   = ()  => setLiveStatus(true);
  es.onerror  = ()  => setLiveStatus(false);
  es.onmessage = (e) => {
    try {
      const data = JSON.parse(e.data);
      if (data.type === 'clear') clearUI();
      else applyPrediction(data, true);
    } catch (err) { console.error('SSE parse error:', err); }
  };
}

// ── Manual prediction validation ─────────────────────────────────────────────
const MANUAL_FIELD_IDS = ['f-ct-alive','f-t-alive','f-ct-hp','f-t-hp','f-time',
                          'f-ct-kills','f-ct-dmg','f-t-kills','f-t-dmg'];

function validateManualForm() {
  const iv = id => {
    const s = document.getElementById(id).value.trim();
    if (!s) return NaN;
    const n = Number(s);
    return Number.isFinite(n) && n === Math.floor(n) ? n : NaN;
  };
  const fv = id => {
    const s = document.getElementById(id).value.trim();
    if (!s) return NaN;
    const n = Number(s);
    return Number.isFinite(n) ? n : NaN;
  };

  const ctAlive     = iv('f-ct-alive');
  const tAlive      = iv('f-t-alive');
  const ctHp        = iv('f-ct-hp');
  const tHp         = iv('f-t-hp');
  const time        = fv('f-time');
  const bombPlanted = document.getElementById('f-bomb').checked;
  const ctKills     = iv('f-ct-kills');
  const ctDmg       = iv('f-ct-dmg');
  const tKills      = iv('f-t-kills');
  const tDmg        = iv('f-t-dmg');

  const errors = {};

  if (isNaN(ctAlive) || ctAlive < 0 || ctAlive > 5)
    errors['f-ct-alive'] = 'Enter a whole number from 0 to 5';
  if (isNaN(tAlive) || tAlive < 0 || tAlive > 5)
    errors['f-t-alive'] = 'Enter a whole number from 0 to 5';

  if (!errors['f-ct-alive'] && !errors['f-t-alive'] && ctAlive === 0 && tAlive === 0)
    errors['f-ct-alive'] = 'At least one side must have a player alive';

  if (!errors['f-ct-alive']) {
    if (isNaN(ctHp) || ctHp < 0)
      errors['f-ct-hp'] = 'Enter a non-negative whole number';
    else if (ctAlive === 0 && ctHp !== 0)
      errors['f-ct-hp'] = 'Must be 0 — no CT players are alive';
    else if (ctAlive > 0 && ctHp < ctAlive)
      errors['f-ct-hp'] = `Minimum ${ctAlive} (at least 1 HP per alive player)`;
    else if (ctHp > ctAlive * 100)
      errors['f-ct-hp'] = `Maximum ${ctAlive * 100} (100 HP per player × ${ctAlive} alive)`;
  }

  if (!errors['f-t-alive']) {
    if (isNaN(tHp) || tHp < 0)
      errors['f-t-hp'] = 'Enter a non-negative whole number';
    else if (tAlive === 0 && tHp !== 0)
      errors['f-t-hp'] = 'Must be 0 — no T players are alive';
    else if (tAlive > 0 && tHp < tAlive)
      errors['f-t-hp'] = `Minimum ${tAlive} (at least 1 HP per alive player)`;
    else if (tHp > tAlive * 100)
      errors['f-t-hp'] = `Maximum ${tAlive * 100} (100 HP per player × ${tAlive} alive)`;
  }

  const maxTime = bombPlanted ? 40 : 115;
  if (isNaN(time) || time < 0 || time > maxTime)
    errors['f-time'] = bombPlanted ? 'Bomb fuse: 0 – 40 s' : 'Round timer: 0 – 115 s';

  if (isNaN(ctKills) || ctKills < 0 || ctKills > 15)
    errors['f-ct-kills'] = '0 – 15  (max 5 kills/round × 3 rounds)';
  if (isNaN(tKills)  || tKills  < 0 || tKills  > 15)
    errors['f-t-kills'] = '0 – 15  (max 5 kills/round × 3 rounds)';
  if (isNaN(ctDmg) || ctDmg < 0 || ctDmg > 3000)
    errors['f-ct-dmg'] = '0 – 3000';
  if (isNaN(tDmg)  || tDmg  < 0 || tDmg  > 3000)
    errors['f-t-dmg'] = '0 – 3000';

  return errors;
}

function applyManualValidation(errors) {
  for (const id of MANUAL_FIELD_IDS) {
    const el = document.getElementById(id);
    const fb = el.nextElementSibling;
    if (errors[id]) {
      el.classList.add('is-invalid');
      if (fb) fb.textContent = errors[id];
    } else {
      el.classList.remove('is-invalid');
    }
  }
  return Object.keys(errors).length === 0;
}

function validateAndApply() {
  if (!formSubmitAttempted) return;
  applyManualValidation(validateManualForm());
}

// ── Manual prediction form ────────────────────────────────────────────────────
document.getElementById('predictForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  formSubmitAttempted = true;
  if (!applyManualValidation(validateManualForm())) return;

  const btn     = document.getElementById('predictBtn');
  const spinner = document.getElementById('predictSpinner');
  const result  = document.getElementById('predictResult');

  btn.disabled = true;
  spinner.classList.remove('d-none');
  result.innerHTML = '';

  try {
    const resp = await fetch('/api/predict', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ctAlive:            +document.getElementById('f-ct-alive').value,
        tAlive:             +document.getElementById('f-t-alive').value,
        ctTotalHp:          +document.getElementById('f-ct-hp').value,
        tTotalHp:           +document.getElementById('f-t-hp').value,
        bombPlanted:        document.getElementById('f-bomb').checked ? 1 : 0,
        remainingTime:      +document.getElementById('f-time').value,
        ctAliveKillsPrev3:  +document.getElementById('f-ct-kills').value,
        ctAliveDamagePrev3: +document.getElementById('f-ct-dmg').value,
        tAliveKillsPrev3:   +document.getElementById('f-t-kills').value,
        tAliveDamagePrev3:  +document.getElementById('f-t-dmg').value,
      }),
    });

    const data = await resp.json();
    if (data.error) {
      result.innerHTML =
        `<div class="alert alert-danger mt-2 py-2 small">${data.error}</div>`;
    } else {
      const pct   = Math.round(data.probCtWin * 100);
      const color = data.probCtWin >= 0.5 ? 'success' : 'danger';
      result.innerHTML = `
        <div class="mt-3">
          <div class="d-flex justify-content-between mb-1 small">
            <span>CT Win probability</span><strong>${pct}%</strong>
          </div>
          <div class="progress" style="height:22px">
            <div class="progress-bar bg-${color} fw-bold" style="width:${pct}%">${pct}%</div>
          </div>
        </div>`;
    }
  } catch (err) {
    result.innerHTML =
      `<div class="alert alert-danger mt-2 py-2 small">Request failed: ${err.message}</div>`;
  } finally {
    btn.disabled = false;
    spinner.classList.add('d-none');
  }
});

// Pre-fill fills with balanced round-start defaults (feature values are not in the prediction stream)
document.getElementById('prefillBtn').addEventListener('click', () => {
  document.getElementById('f-ct-alive').value  = 5;
  document.getElementById('f-t-alive').value   = 5;
  document.getElementById('f-ct-hp').value     = 500;
  document.getElementById('f-t-hp').value      = 500;
  document.getElementById('f-bomb').checked    = false;
  document.getElementById('f-time').max        = 115;
  document.getElementById('f-time').value      = 115;
  document.getElementById('f-ct-kills').value  = 0;
  document.getElementById('f-ct-dmg').value    = 0;
  document.getElementById('f-t-kills').value   = 0;
  document.getElementById('f-t-dmg').value     = 0;
  validateAndApply();
});

// Live validation after first submit attempt
MANUAL_FIELD_IDS.forEach(id => {
  document.getElementById(id).addEventListener('input', validateAndApply);
});
document.getElementById('f-bomb').addEventListener('change', () => {
  document.getElementById('f-time').max = document.getElementById('f-bomb').checked ? 40 : 115;
  validateAndApply();
});

// ── Bootstrap ─────────────────────────────────────────────────────────────────
(async () => {
  document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => {
    new bootstrap.Tooltip(el);
  });
  initMainChart();
  setLiveStatus(false);
  await seedHistory();
  connectSSE();
})();
