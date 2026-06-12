/* PANDA Charting - Binance spot/futures chart with trade bubbles, candle delta,
   live depth chart and TradingView-style auto-center (A) button. */

'use strict';

const ENDPOINTS = {
  spot:    { rest: 'https://api.binance.com/api/v3/klines',  ws: 'wss://stream.binance.com:9443/stream?streams=' },
  futures: { rest: 'https://fapi.binance.com/fapi/v1/klines', ws: 'wss://fstream.binance.com/stream?streams=' }
};

const TF_SECONDS = { '1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400 };

// ---------- state ----------
const state = {
  market: 'spot',
  symbol: 'BTCUSDT',
  tf: '5m',
  thresholdUsd: 100000,
  autoCenter: true,
  ws: null,
  wsGen: 0,            // generation counter to ignore stale sockets
  bubbles: [],         // { time (candle bucket, sec), price, usd, side }
  depth: { bids: [], asks: [] },
  lastCandle: null
};

// ---------- DOM ----------
const $ = (id) => document.getElementById(id);
const statusEl = $('status');
const deltaInfoEl = $('deltaInfo');
const bubbleCanvas = $('bubbleCanvas');
const depthCanvas = $('depthCanvas');

// ---------- chart ----------
const chart = LightweightCharts.createChart($('chart'), {
  layout: { background: { color: '#0b0e14' }, textColor: '#aab3c5' },
  grid: { vertLines: { color: '#151b28' }, horzLines: { color: '#151b28' } },
  rightPriceScale: { borderColor: '#1d2433', scaleMargins: { top: 0.08, bottom: 0.22 } },
  timeScale: { borderColor: '#1d2433', timeVisible: true, secondsVisible: false, rightOffset: 8 },
  crosshair: { mode: LightweightCharts.CrosshairMode.Normal }
});

const candleSeries = chart.addCandlestickSeries({
  upColor: '#2ecc71', downColor: '#e74c3c',
  wickUpColor: '#2ecc71', wickDownColor: '#e74c3c',
  borderVisible: false
});

// Volume-delta histogram on its own scale at the bottom of the pane.
const deltaSeries = chart.addHistogramSeries({
  priceScaleId: 'delta',
  priceFormat: { type: 'volume' },
  lastValueVisible: false,
  priceLineVisible: false
});
chart.priceScale('delta').applyOptions({ scaleMargins: { top: 0.84, bottom: 0 } });

// ---------- helpers ----------
function setStatus(text, cls) {
  statusEl.textContent = text;
  statusEl.className = 'status' + (cls ? ' ' + cls : '');
}

function bucketTime(ms) {
  const s = TF_SECONDS[state.tf];
  return Math.floor(ms / 1000 / s) * s;
}

function deltaColor(d) { return d >= 0 ? 'rgba(46,204,113,0.55)' : 'rgba(231,76,60,0.55)'; }

function fmtUsd(v) {
  if (v >= 1e9) return (v / 1e9).toFixed(2) + 'B';
  if (v >= 1e6) return (v / 1e6).toFixed(2) + 'M';
  if (v >= 1e3) return (v / 1e3).toFixed(1) + 'K';
  return v.toFixed(0);
}

// ---------- historical klines ----------
async function loadHistory() {
  const { rest } = ENDPOINTS[state.market];
  const url = `${rest}?symbol=${state.symbol}&interval=${state.tf}&limit=500`;
  const res = await fetch(url);
  if (!res.ok) throw new Error('klines HTTP ' + res.status);
  const rows = await res.json();

  const candles = [];
  const deltas = [];
  for (const r of rows) {
    const time = Math.floor(r[0] / 1000);
    const vol = parseFloat(r[5]);
    const takerBuy = parseFloat(r[9]);
    const delta = 2 * takerBuy - vol; // buy vol - sell vol
    candles.push({ time, open: +r[1], high: +r[2], low: +r[3], close: +r[4] });
    deltas.push({ time, value: delta, color: deltaColor(delta) });
  }
  candleSeries.setData(candles);
  deltaSeries.setData(deltas);
  state.lastCandle = candles[candles.length - 1] || null;
  if (state.autoCenter) autoCenterNow();
}

// ---------- websocket ----------
function connectWs() {
  if (state.ws) { try { state.ws.close(); } catch (_) {} }
  const gen = ++state.wsGen;
  const s = state.symbol.toLowerCase();
  const streams = [`${s}@kline_${state.tf}`, `${s}@aggTrade`, `${s}@depth20@100ms`].join('/');
  const ws = new WebSocket(ENDPOINTS[state.market].ws + streams);
  state.ws = ws;

  ws.onopen = () => { if (gen === state.wsGen) setStatus(`${state.market.toUpperCase()} ${state.symbol} live`, 'ok'); };

  ws.onmessage = (ev) => {
    if (gen !== state.wsGen) return;
    const msg = JSON.parse(ev.data);
    const data = msg.data || msg;
    const stream = msg.stream || '';
    if (stream.includes('@kline') || data.e === 'kline') onKline(data.k);
    else if (stream.includes('@aggTrade') || data.e === 'aggTrade') onAggTrade(data);
    else if (stream.includes('@depth') || data.e === 'depthUpdate' || data.bids) onDepth(data);
  };

  ws.onclose = () => {
    if (gen !== state.wsGen) return;
    setStatus('disconnected - reconnecting\u2026', 'err');
    setTimeout(() => { if (gen === state.wsGen) connectWs(); }, 2000);
  };
  ws.onerror = () => { try { ws.close(); } catch (_) {} };
}

function onKline(k) {
  const time = Math.floor(k.t / 1000);
  const candle = { time, open: +k.o, high: +k.h, low: +k.l, close: +k.c };
  candleSeries.update(candle);
  state.lastCandle = candle;

  const vol = parseFloat(k.v);
  const takerBuy = parseFloat(k.V);
  const delta = 2 * takerBuy - vol;
  deltaSeries.update({ time, value: delta, color: deltaColor(delta) });
  deltaInfoEl.textContent = `\u0394 ${delta >= 0 ? '+' : ''}${delta.toFixed(2)}`;
  deltaInfoEl.style.color = delta >= 0 ? '#2ecc71' : '#e74c3c';

  if (state.autoCenter) autoCenterNow();
  drawBubbles();
}

function onAggTrade(t) {
  const price = parseFloat(t.p);
  const qty = parseFloat(t.q);
  const usd = price * qty;
  if (usd < state.thresholdUsd) return;
  state.bubbles.push({
    time: bucketTime(t.T),
    price,
    usd,
    side: t.m ? 'sell' : 'buy' // m=true: buyer is maker => aggressive sell
  });
  if (state.bubbles.length > 600) state.bubbles.splice(0, state.bubbles.length - 600);
  drawBubbles();
}

function onDepth(d) {
  const bids = d.bids || d.b || [];
  const asks = d.asks || d.a || [];
  state.depth.bids = bids.map((x) => [parseFloat(x[0]), parseFloat(x[1])]);
  state.depth.asks = asks.map((x) => [parseFloat(x[0]), parseFloat(x[1])]);
  drawDepth();
}

// ---------- bubble overlay ----------
function sizeCanvas(canvas) {
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  if (canvas.width !== Math.round(rect.width * dpr) || canvas.height !== Math.round(rect.height * dpr)) {
    canvas.width = Math.round(rect.width * dpr);
    canvas.height = Math.round(rect.height * dpr);
  }
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return ctx;
}

function drawBubbles() {
  const ctx = sizeCanvas(bubbleCanvas);
  const rect = bubbleCanvas.getBoundingClientRect();
  ctx.clearRect(0, 0, rect.width, rect.height);

  const ts = chart.timeScale();
  for (const b of state.bubbles) {
    const x = ts.timeToCoordinate(b.time);
    const y = candleSeries.priceToCoordinate(b.price);
    if (x === null || y === null) continue;
    const r = Math.min(34, 4 + Math.sqrt(b.usd / Math.max(state.thresholdUsd, 1)) * 5);
    ctx.beginPath();
    ctx.arc(x, y, r, 0, Math.PI * 2);
    ctx.fillStyle = b.side === 'buy' ? 'rgba(46,204,113,0.30)' : 'rgba(231,76,60,0.30)';
    ctx.fill();
    ctx.lineWidth = 1.5;
    ctx.strokeStyle = b.side === 'buy' ? '#2ecc71' : '#e74c3c';
    ctx.stroke();
    if (r >= 12) {
      ctx.fillStyle = '#e8edf5';
      ctx.font = '10px sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText('$' + fmtUsd(b.usd), x, y + 3);
    }
  }
}

chart.timeScale().subscribeVisibleTimeRangeChange(drawBubbles);

// ---------- depth chart ----------
function drawDepth() {
  const ctx = sizeCanvas(depthCanvas);
  const rect = depthCanvas.getBoundingClientRect();
  const W = rect.width, H = rect.height;
  ctx.clearRect(0, 0, W, H);

  const { bids, asks } = state.depth;
  if (!bids.length || !asks.length) return;

  // cumulative volumes
  let cum = 0;
  const cumBids = bids.map(([p, q]) => { cum += q; return [p, cum]; });
  cum = 0;
  const cumAsks = asks.map(([p, q]) => { cum += q; return [p, cum]; });
  const maxCum = Math.max(cumBids[cumBids.length - 1][1], cumAsks[cumAsks.length - 1][1]);

  const minP = cumBids[cumBids.length - 1][0];
  const maxP = cumAsks[cumAsks.length - 1][0];
  const yOf = (p) => H - ((p - minP) / (maxP - minP || 1)) * H;
  const xOf = (v) => (v / (maxCum || 1)) * (W - 6);

  const fillSide = (levels, color, stroke) => {
    ctx.beginPath();
    ctx.moveTo(0, yOf(levels[0][0]));
    for (const [p, v] of levels) ctx.lineTo(xOf(v), yOf(p));
    ctx.lineTo(0, yOf(levels[levels.length - 1][0]));
    ctx.closePath();
    ctx.fillStyle = color;
    ctx.fill();
    ctx.strokeStyle = stroke;
    ctx.stroke();
  };
  fillSide(cumBids, 'rgba(46,204,113,0.22)', '#2ecc71');
  fillSide(cumAsks, 'rgba(231,76,60,0.22)', '#e74c3c');

  // mid price line + label
  const mid = (bids[0][0] + asks[0][0]) / 2;
  const yMid = yOf(mid);
  ctx.strokeStyle = '#4da3ff';
  ctx.setLineDash([4, 3]);
  ctx.beginPath();
  ctx.moveTo(0, yMid);
  ctx.lineTo(W, yMid);
  ctx.stroke();
  ctx.setLineDash([]);
  ctx.fillStyle = '#4da3ff';
  ctx.font = '11px sans-serif';
  ctx.textAlign = 'right';
  ctx.fillText(mid.toFixed(2), W - 4, yMid - 4);
}

// ---------- auto-center (A button) ----------
function autoCenterNow() {
  chart.timeScale().scrollToRealTime();
  candleSeries.priceScale().applyOptions({ autoScale: true });
}

function setAutoCenter(on) {
  state.autoCenter = on;
  $('autoBtn').classList.toggle('active', on);
  if (on) autoCenterNow();
}

$('autoBtn').addEventListener('click', () => setAutoCenter(!state.autoCenter));

// any manual pan/zoom disables auto mode (like TradingView)
const chartEl = $('chart');
chartEl.addEventListener('mousedown', () => setAutoCenter(false));
chartEl.addEventListener('wheel', () => setAutoCenter(false), { passive: true });
chartEl.addEventListener('touchstart', () => setAutoCenter(false), { passive: true });

// ---------- toolbar wiring ----------
document.querySelectorAll('#marketToggle button').forEach((btn) => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('#marketToggle button').forEach((b) => b.classList.remove('active'));
    btn.classList.add('active');
    state.market = btn.dataset.market;
    reload();
  });
});

$('symbolInput').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    state.symbol = e.target.value.trim().toUpperCase();
    reload();
  }
});

$('tfSelect').addEventListener('change', (e) => {
  state.tf = e.target.value;
  reload();
});

$('thresholdInput').addEventListener('change', (e) => {
  state.thresholdUsd = Math.max(0, parseFloat(e.target.value) || 0);
  state.bubbles = state.bubbles.filter((b) => b.usd >= state.thresholdUsd);
  drawBubbles();
});

// ---------- lifecycle ----------
async function reload() {
  setStatus('loading\u2026');
  state.bubbles = [];
  state.depth = { bids: [], asks: [] };
  drawBubbles();
  drawDepth();
  try {
    await loadHistory();
    connectWs();
    setAutoCenter(true);
  } catch (err) {
    setStatus('error: ' + err.message, 'err');
  }
}

window.addEventListener('resize', () => {
  chart.applyOptions({});
  drawBubbles();
  drawDepth();
});

reload();