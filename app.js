/* PANDA Charting - Binance USD-M futures chart with live tick-driven candles,
   big-trade bubbles, candle delta and TradingView-style auto-center (A) button. */

'use strict';

const ENDPOINTS = {
  rest: 'https://fapi.binance.com/fapi/v1/klines',
  ticker: 'https://fapi.binance.com/fapi/v1/ticker/24hr',
  ws: 'wss://fstream.binance.com/stream?streams='
};

const TF_SECONDS = { '1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400 };

// ---------- state ----------
const state = {
  symbol: 'BTCUSDT',
  tf: '5m',
  thresholdUsd: 100000,
  autoCenter: true,
  depthVisible: true,
  ws: null,
  wsGen: 0,            // generation counter to ignore stale sockets
  bubbles: [],         // { tms (trade time, ms), price, usd, side }
  lastCandle: null,
  candleDirty: false,
  depthDirty: false,
  orderBook: { bids: [], asks: [], mid: null },
  topSymbols: []
};

// ---------- DOM ----------
const $ = (id) => document.getElementById(id);
const statusEl = $('status');
const deltaInfoEl = $('deltaInfo');
const depthInfoEl = $('depthInfo');
const bubbleCanvas = $('bubbleCanvas');
const depthCanvas = $('depthCanvas');

// ---------- chart ----------
const chart = LightweightCharts.createChart($('chart'), {
  autoSize: true,
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

// ---------- bubble persistence (survive page refresh) ----------
function bubbleKey() { return `panda.bubbles.futures.${state.symbol}`; }

function saveBubbles() {
  try { localStorage.setItem(bubbleKey(), JSON.stringify(state.bubbles.slice(-600))); } catch (_) {}
}

function loadBubbles() {
  try {
    const saved = JSON.parse(localStorage.getItem(bubbleKey())) || [];
    state.bubbles = saved.filter((b) => b && b.tms && typeof b.usd === 'number');
  } catch (_) {
    state.bubbles = [];
  }
}

// ---------- historical klines ----------
async function loadHistory() {
  const url = `${ENDPOINTS.rest}?symbol=${state.symbol}&interval=${state.tf}&limit=500`;
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
  const ws = new WebSocket(ENDPOINTS.ws + streams);
  state.ws = ws;

  ws.onopen = () => { if (gen === state.wsGen) setStatus(`FUTURES ${state.symbol} live`, 'ok'); };

  ws.onmessage = (ev) => {
    if (gen !== state.wsGen) return;
    const msg = JSON.parse(ev.data);
    const data = msg.data || msg;
    const stream = msg.stream || '';
    if (stream.includes('@kline') || data.e === 'kline') onKline(data.k);
    else if (stream.includes('@aggTrade') || data.e === 'aggTrade') onAggTrade(data);
    else if (stream.includes('@depth') || data.e === 'depthUpdate') onDepth(data);
  };

  ws.onclose = () => {
    if (gen !== state.wsGen) return;
    setStatus('disconnected - reconnecting\u2026', 'err');
    setTimeout(() => { if (gen === state.wsGen) connectWs(); }, 2000);
  };
  ws.onerror = () => { try { ws.close(); } catch (_) {} };
}

function updateLiveCandle(price, ms) {
  const bt = bucketTime(ms);
  const c = state.lastCandle;
  if (!c || bt > c.time) {
    state.lastCandle = { time: bt, open: price, high: price, low: price, close: price };
  } else if (bt === c.time) {
    state.lastCandle = {
      time: c.time,
      open: c.open,
      high: Math.max(c.high, price),
      low: Math.min(c.low, price),
      close: price
    };
  }
  state.candleDirty = true;
}

function onKline(k) {
  const time = Math.floor(k.t / 1000);
  const close = state.orderBook.mid || +k.c;
  const candle = { time, open: +k.o, high: +k.h, low: +k.l, close };
  // authoritative candle from Binance; merge with tick/depth-built close and extremes
  const c = state.lastCandle;
  if (c && c.time === time) {
    candle.high = Math.max(candle.high, c.high, close);
    candle.low = Math.min(candle.low, c.low, close);
  }
  state.lastCandle = candle;
  state.candleDirty = true;

  const vol = parseFloat(k.v);
  const takerBuy = parseFloat(k.V);
  const delta = 2 * takerBuy - vol;
  deltaSeries.update({ time, value: delta, color: deltaColor(delta) });
  deltaInfoEl.textContent = `\u0394 ${delta >= 0 ? '+' : ''}${delta.toFixed(2)}`;
  deltaInfoEl.style.color = delta >= 0 ? '#2ecc71' : '#e74c3c';
}

function onAggTrade(t) {
  const price = parseFloat(t.p);
  const qty = parseFloat(t.q);
  const usd = price * qty;

  // Build/extend the live candle from every tick so candles move in real time.
  updateLiveCandle(price, t.T);

  // record big trades as bubbles
  if (usd < state.thresholdUsd) return;
  state.bubbles.push({
    tms: t.T, // raw trade time (ms) so bubbles survive timeframe changes
    price,
    usd,
    side: t.m ? 'sell' : 'buy' // m=true: buyer is maker => aggressive sell
  });
  if (state.bubbles.length > 600) state.bubbles.splice(0, state.bubbles.length - 600);
  saveBubbles();
}

function onDepth(d) {
  const bids = (d.b || d.bids || [])
    .map((r) => ({ price: +r[0], qty: +r[1] }))
    .filter((r) => r.price > 0 && r.qty > 0)
    .sort((a, b) => b.price - a.price)
    .slice(0, 20);
  const asks = (d.a || d.asks || [])
    .map((r) => ({ price: +r[0], qty: +r[1] }))
    .filter((r) => r.price > 0 && r.qty > 0)
    .sort((a, b) => a.price - b.price)
    .slice(0, 20);
  if (!bids.length || !asks.length) return;

  const bestBid = bids[0].price;
  const bestAsk = asks[0].price;
  const mid = (bestBid + bestAsk) / 2;
  state.orderBook = { bids, asks, mid };
  state.depthDirty = true;
  depthInfoEl.textContent = `Depth ${mid.toFixed(mid >= 100 ? 2 : 5)}`;
  updateLiveCandle(mid, d.E || Date.now());
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
  if (!rect.width || !rect.height) return;

  const ts = chart.timeScale();
  const visible = ts.getVisibleRange();
  for (const b of state.bubbles) {
    if (b.usd < state.thresholdUsd) continue;
    const bubbleTime = bucketTime(b.tms);
    if (visible && (bubbleTime < visible.from || bubbleTime > visible.to)) continue;
    const x = ts.timeToCoordinate(bubbleTime);
    const y = candleSeries.priceToCoordinate(b.price);
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
    // radius scales with trade size: bigger volume => bigger bubble
    const r = Math.min(64, 6 + Math.sqrt(b.usd / Math.max(state.thresholdUsd, 1)) * 9);
    ctx.beginPath();
    ctx.arc(x, y, r, 0, Math.PI * 2);
    // green = buy order executed, red = sell executed
    ctx.fillStyle = b.side === 'buy' ? 'rgba(46,204,113,0.45)' : 'rgba(231,76,60,0.45)';
    ctx.fill();
    ctx.lineWidth = 2;
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

// ---------- depth chart ----------
function drawDepth() {
  if (!state.depthVisible) return;
  const ctx = sizeCanvas(depthCanvas);
  const rect = depthCanvas.getBoundingClientRect();
  ctx.clearRect(0, 0, rect.width, rect.height);
  if (!rect.width || !rect.height) return;

  const { bids, asks, mid } = state.orderBook;
  if (!bids.length || !asks.length) {
    ctx.fillStyle = '#8b94a7';
    ctx.font = '13px sans-serif';
    ctx.fillText('Waiting for depth...', 12, 24);
    return;
  }

  const build = (rows) => {
    let total = 0;
    return rows.map((r) => ({ price: r.price, total: total += r.qty }));
  };
  const bidCum = build(bids).reverse();
  const askCum = build(asks);
  const all = bidCum.concat(askCum);
  const minPrice = Math.min(...all.map((r) => r.price));
  const maxPrice = Math.max(...all.map((r) => r.price));
  const maxTotal = Math.max(...all.map((r) => r.total));
  const pad = 28;
  const xFor = (p) => pad + ((p - minPrice) / Math.max(maxPrice - minPrice, 1)) * (rect.width - pad * 2);
  const yFor = (v) => rect.height - pad - (v / Math.max(maxTotal, 1)) * (rect.height - pad * 2);

  function area(points, fill, stroke) {
    ctx.beginPath();
    ctx.moveTo(xFor(points[0].price), rect.height - pad);
    for (const p of points) ctx.lineTo(xFor(p.price), yFor(p.total));
    ctx.lineTo(xFor(points[points.length - 1].price), rect.height - pad);
    ctx.closePath();
    ctx.fillStyle = fill;
    ctx.fill();
    ctx.beginPath();
    for (const [i, p] of points.entries()) {
      const x = xFor(p.price), y = yFor(p.total);
      if (i) ctx.lineTo(x, y); else ctx.moveTo(x, y);
    }
    ctx.strokeStyle = stroke;
    ctx.lineWidth = 2;
    ctx.stroke();
  }

  area(bidCum, 'rgba(46,204,113,0.18)', '#2ecc71');
  area(askCum, 'rgba(231,76,60,0.18)', '#e74c3c');

  const midX = xFor(mid);
  ctx.strokeStyle = '#4da3ff';
  ctx.setLineDash([4, 4]);
  ctx.beginPath();
  ctx.moveTo(midX, pad / 2);
  ctx.lineTo(midX, rect.height - pad / 2);
  ctx.stroke();
  ctx.setLineDash([]);
  ctx.fillStyle = '#e8edf5';
  ctx.font = '12px sans-serif';
  ctx.textAlign = 'center';
  ctx.fillText(mid.toFixed(mid >= 100 ? 2 : 5), midX, 18);
  ctx.textAlign = 'left';
  ctx.fillStyle = '#2ecc71';
  ctx.fillText(`Bid ${bids[0].price}`, 10, rect.height - 10);
  ctx.fillStyle = '#e74c3c';
  ctx.textAlign = 'right';
  ctx.fillText(`Ask ${asks[0].price}`, rect.width - 10, rect.height - 10);
}

function setDepthVisible(on) {
  state.depthVisible = on;
  $('depthPanel').classList.toggle('hidden', !on);
  $('depthToggle').classList.toggle('active', on);
  $('depthToggle').textContent = on ? 'Depth' : 'Show Depth';
  setTimeout(() => {
    chart.applyOptions({ autoSize: true });
    drawBubbles();
    drawDepth();
  }, 0);
}

$('depthToggle').addEventListener('click', () => setDepthVisible(!state.depthVisible));
$('depthHide').addEventListener('click', () => setDepthVisible(false));

// ---------- render loop ----------
// Candles + trade bubbles repaint every ~16ms (within the 10-20ms window).
setInterval(() => {
  if (state.candleDirty && state.lastCandle) {
    candleSeries.update(state.lastCandle);
    state.candleDirty = false;
    if (state.autoCenter) autoCenterNow();
  }
  drawBubbles();
  if (state.depthDirty) {
    drawDepth();
    state.depthDirty = false;
  }
}, 16);

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

// ---------- symbol search: top 50 USDT pairs by 24h volume ----------
const symInput = $('symbolInput');
const symList = $('symbolList');

async function loadTopSymbols() {
  try {
    const res = await fetch(ENDPOINTS.ticker);
    if (!res.ok) return;
    const rows = await res.json();
    state.topSymbols = rows
      .filter((r) => r.symbol.endsWith('USDT'))
      .map((r) => ({
        symbol: r.symbol,
        vol: parseFloat(r.quoteVolume),
        chg: parseFloat(r.priceChangePercent)
      }))
      .sort((a, b) => b.vol - a.vol)
      .slice(0, 50);
  } catch (_) {}
}

function renderSymbolList(filter) {
  const q = (filter || '').trim().toUpperCase();
  const rows = state.topSymbols.filter((s) => !q || s.symbol.includes(q));
  symList.innerHTML = rows.map((s) => `
    <div class="symRow" data-sym="${s.symbol}">
      <span class="symName">${s.symbol}</span>
      <span class="symVol">$${fmtUsd(s.vol)}</span>
      <span class="symChg ${s.chg >= 0 ? 'up' : 'down'}">${s.chg >= 0 ? '+' : ''}${s.chg.toFixed(2)}%</span>
    </div>`).join('');
  symList.style.display = rows.length ? 'block' : 'none';
}

function pickSymbol(sym) {
  if (!sym) return;
  symInput.value = sym;
  state.symbol = sym;
  symList.style.display = 'none';
  reload();
}

symInput.addEventListener('focus', async () => {
  symInput.select(); // typing replaces the old symbol
  if (!state.topSymbols.length) await loadTopSymbols();
  renderSymbolList(''); // show the full top-50 list by default
});
symInput.addEventListener('input', () => renderSymbolList(symInput.value));
symInput.addEventListener('blur', () => setTimeout(() => { symList.style.display = 'none'; }, 150));
symInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') pickSymbol(e.target.value.trim().toUpperCase());
});
symList.addEventListener('mousedown', (e) => {
  const row = e.target.closest('.symRow');
  if (row) pickSymbol(row.dataset.sym);
});

// ---------- toolbar wiring ----------
$('tfSelect').addEventListener('change', (e) => {
  state.tf = e.target.value;
  reload();
});

// threshold applies instantly while typing (no need to press Enter or blur)
$('thresholdInput').addEventListener('input', (e) => {
  state.thresholdUsd = Math.max(0, parseFloat(e.target.value) || 0);
  drawBubbles();
});

// ---------- lifecycle ----------
async function reload() {
  setStatus('loading\u2026');
  state.orderBook = { bids: [], asks: [], mid: null };
  state.depthDirty = true;
  loadBubbles(); // restore persisted bubbles for this symbol
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
  drawBubbles();
  drawDepth();
});
chart.timeScale().subscribeVisibleTimeRangeChange(drawBubbles);

loadTopSymbols();
reload();
