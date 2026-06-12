/* PANDA Charting - Binance USD-M futures chart with live tick-driven candles,
   big-trade bubbles, candle delta and TradingView-style auto-center (A) button. */

'use strict';

const ENDPOINTS = {
  rest: 'https://fapi.binance.com/fapi/v1/klines',
  ticker: 'https://fapi.binance.com/fapi/v1/ticker/24hr',
  ws: 'wss://fstream.binance.com/stream?streams='
};

const TF_SECONDS = { '1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400 };
const DEFAULT_BIG_TRADE_USD = 50000;

// ---------- state ----------
const state = {
  symbol: 'BTCUSDT',
  tf: '5m',
  thresholdUsd: 100000,
  domThresholdUsd: DEFAULT_BIG_TRADE_USD,
  autoCenter: true,
  depthVisible: true,
  ws: null,
  depthWs: null,
  wsGen: 0,            // generation counter to ignore stale sockets
  bubbles: [],         // individual trades >= domThresholdUsd: { tms, price, usd, side }
  domTrades: [],
  liveDeltas: new Map(),
  candlesByTime: new Map(),
  deltasByTime: new Map(),
  lastCandle: null,
  candleDirty: false,
  markersDirty: false,
  depthDirty: false,
  orderBook: { bids: [], asks: [], mid: null },
  topSymbols: []
};

// ---------- DOM ----------
const $ = (id) => document.getElementById(id);
const statusEl = $('status');
const deltaInfoEl = $('deltaInfo');
const bubbleInfoEl = $('bubbleInfo');
const depthInfoEl = $('depthInfo');
const ohlcInfoEl = $('ohlcInfo');
const domRowsEl = $('domRows');
const settingsModalEl = $('settingsModal');
const domThresholdInputEl = $('domThresholdInput');
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

function fmtPrice(v) { return Number.isFinite(v) ? (v >= 100 ? v.toFixed(2) : v.toFixed(5)) : '-'; }
function timeKey(time) {
  if (typeof time === 'number') return time;
  if (time && typeof time === 'object') return Date.UTC(time.year, time.month - 1, time.day) / 1000;
  return null;
}

// ---------- bubble persistence (survive page refresh) ----------
function bubbleKey() { return `panda.bubbles.futures.${state.symbol}`; }

function saveBubbles() {
  try { localStorage.setItem(bubbleKey(), JSON.stringify(state.bubbles.slice(-1800))); } catch (_) {}
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
  state.candlesByTime.clear();
  state.deltasByTime.clear();
  for (const r of rows) {
    const time = Math.floor(r[0] / 1000);
    const vol = parseFloat(r[5]);
    const takerBuy = parseFloat(r[9]);
    const delta = 2 * takerBuy - vol; // buy vol - sell vol
    const candle = { time, open: +r[1], high: +r[2], low: +r[3], close: +r[4] };
    candles.push(candle);
    deltas.push({ time, value: delta, color: deltaColor(delta) });
    state.candlesByTime.set(time, candle);
    state.deltasByTime.set(time, delta);
  }
  candleSeries.setData(candles);
  deltaSeries.setData(deltas);
  state.lastCandle = candles[candles.length - 1] || null;
  if (state.autoCenter) autoCenterNow();
}

// ---------- websocket ----------
function connectWs() {
  if (state.ws) { try { state.ws.close(); } catch (_) {} }
  if (state.depthWs) { try { state.depthWs.close(); } catch (_) {} }
  const gen = ++state.wsGen;
  const s = state.symbol.toLowerCase();

  // Keep price/trade data on its own socket so a depth issue can never freeze candles.
  const marketStreams = [`${s}@kline_${state.tf}`, `${s}@trade`, `${s}@bookTicker`].join('/');
  const ws = new WebSocket(ENDPOINTS.ws + marketStreams);
  state.ws = ws;

  ws.onopen = () => { if (gen === state.wsGen) setStatus(`FUTURES ${state.symbol} live`, 'ok'); };

  ws.onmessage = (ev) => {
    if (gen !== state.wsGen) return;
    const msg = JSON.parse(ev.data);
    const data = msg.data || msg;
    const stream = msg.stream || '';
    if (stream.includes('@kline') || data.e === 'kline') onKline(data.k);
    else if (stream.includes('@trade') || data.e === 'trade') onTrade(data);
    else if (stream.includes('@bookTicker') || data.e === 'bookTicker') onBookTicker(data);
  };

  ws.onclose = () => {
    if (gen !== state.wsGen) return;
    setStatus('price disconnected - reconnecting\u2026', 'err');
    setTimeout(() => { if (gen === state.wsGen) connectWs(); }, 2000);
  };
  ws.onerror = () => { try { ws.close(); } catch (_) {} };

  connectDepthWs(gen, s);
}

function connectDepthWs(gen, s) {
  // Partial book depth stream for drawing; separate from live candles.
  const depthWs = new WebSocket(ENDPOINTS.ws + `${s}@depth20@500ms`);
  state.depthWs = depthWs;

  depthWs.onmessage = (ev) => {
    if (gen !== state.wsGen) return;
    const msg = JSON.parse(ev.data);
    onDepth(msg.data || msg);
  };
  depthWs.onclose = () => {
    if (gen !== state.wsGen) return;
    state.depthDirty = true;
    setTimeout(() => { if (gen === state.wsGen) connectDepthWs(gen, s); }, 2000);
  };
  depthWs.onerror = () => { try { depthWs.close(); } catch (_) {} };
}

function updateLiveCandle(price, ms) {
  if (!Number.isFinite(price) || price <= 0) return;
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
  if (state.lastCandle) state.candlesByTime.set(state.lastCandle.time, state.lastCandle);
  state.candleDirty = true;
}

function onKline(k) {
  const time = Math.floor(k.t / 1000);
  const open = +k.o;
  const high = +k.h;
  const low = +k.l;
  const kClose = +k.c;
  if (![open, high, low, kClose].every((v) => Number.isFinite(v) && v > 0)) return;
  const close = state.orderBook.mid || kClose;
  const candle = { time, open, high, low, close };
  // authoritative candle from Binance; merge with tick/depth-built close and extremes
  const c = state.lastCandle;
  if (c && c.time === time) {
    candle.high = Math.max(candle.high, c.high, close);
    candle.low = Math.min(candle.low, c.low, close);
  }
  state.lastCandle = candle;
  state.candlesByTime.set(time, candle);
  state.candleDirty = true;

  const vol = parseFloat(k.v);
  const takerBuy = parseFloat(k.V);
  const exchangeDelta = 2 * takerBuy - vol;
  const liveDelta = state.liveDeltas.get(time);
  const delta = Number.isFinite(liveDelta) ? liveDelta : exchangeDelta;
  state.deltasByTime.set(time, delta);
  deltaSeries.update({ time, value: delta, color: deltaColor(delta) });
  deltaInfoEl.textContent = `\u0394 ${delta >= 0 ? '+' : ''}${delta.toFixed(2)}`;
  deltaInfoEl.style.color = delta >= 0 ? '#2ecc71' : '#e74c3c';
}

function onBookTicker(t) {
  const bid = parseFloat(t.b);
  const ask = parseFloat(t.a);
  if (!Number.isFinite(bid) || !Number.isFinite(ask) || bid <= 0 || ask <= 0) return;
  const mid = (bid + ask) / 2;
  state.orderBook.mid = mid;
  depthInfoEl.textContent = `Price ${mid.toFixed(mid >= 100 ? 2 : 5)}`;
  updateLiveCandle(mid, Date.now());
}

function renderDomTrades() {
  if (!domRowsEl) return;
  domRowsEl.innerHTML = state.domTrades.map((t) => {
    const time = new Date(t.tms).toLocaleTimeString([], { hour12: false });
    return `<div class="domRow ${t.side}"><span>${time}</span><span>${t.side.toUpperCase()} ${fmtPrice(t.price)}</span><span class="domUsd">$${fmtUsd(t.usd)}</span></div>`;
  }).join('') || `<div class="domRow"><span></span><span>Waiting for individual $${fmtUsd(state.domThresholdUsd)}+ trades</span><span></span></div>`;
}

function recordTrade(t) {
  const price = parseFloat(t.p);
  const qty = parseFloat(t.q);
  if (!Number.isFinite(price) || !Number.isFinite(qty) || price <= 0 || qty <= 0) return;

  const tms = t.T || Date.now();
  const side = t.m ? 'sell' : 'buy';
  const usd = price * qty;
  updateLiveCandle(price, tms);

  const time = bucketTime(tms);
  const signedQty = side === 'buy' ? qty : -qty;
  const liveDelta = (state.liveDeltas.get(time) || 0) + signedQty;
  state.liveDeltas.set(time, liveDelta);
  state.deltasByTime.set(time, liveDelta);
  deltaSeries.update({ time, value: liveDelta, color: deltaColor(liveDelta) });
  deltaInfoEl.textContent = `Δ ${liveDelta >= 0 ? '+' : ''}${liveDelta.toFixed(2)}`;
  deltaInfoEl.style.color = liveDelta >= 0 ? '#2ecc71' : '#e74c3c';

  if (usd < state.domThresholdUsd) return;
  const trade = { tms, price, usd, side };
  state.bubbles.push(trade);
  state.domTrades.unshift(trade);
  if (state.bubbles.length > 1800) state.bubbles.splice(0, state.bubbles.length - 1800);
  if (state.domTrades.length > 80) state.domTrades.splice(80);
  state.markersDirty = true;
  updateBubbleMarkers();
  renderDomTrades();
  saveBubbles();
}

function onTrade(t) {
  recordTrade(t);
}

function onDepth(d) {
  const rawBids = d.bids || d.b || [];
  const rawAsks = d.asks || d.a || [];
  const bids = rawBids
    .map((r) => ({ price: +r[0], qty: +r[1] }))
    .filter((r) => Number.isFinite(r.price) && Number.isFinite(r.qty) && r.price > 0 && r.qty > 0)
    .sort((a, b) => b.price - a.price)
    .slice(0, 20);
  const asks = rawAsks
    .map((r) => ({ price: +r[0], qty: +r[1] }))
    .filter((r) => Number.isFinite(r.price) && Number.isFinite(r.qty) && r.price > 0 && r.qty > 0)
    .sort((a, b) => a.price - b.price)
    .slice(0, 20);
  if (!bids.length || !asks.length) return;

  const bestBid = bids[0].price;
  const bestAsk = asks[0].price;
  const mid = (bestBid + bestAsk) / 2;
  state.orderBook = { bids, asks, mid };
  state.depthDirty = true;
  depthInfoEl.textContent = `Depth ${mid.toFixed(mid >= 100 ? 2 : 5)}`;
  updateLiveCandle(mid, d.E || d.T || Date.now());
}

// ---------- bubble overlay ----------
function getBubbleGroups() {
  const groups = new Map();
  for (const b of state.bubbles) {
    if (b.usd < state.domThresholdUsd) continue;
    const time = bucketTime(b.tms);
    const key = `${time}.${b.side}`;
    const g = groups.get(key) || { time, side: b.side, usd: 0, maxUsd: 0, priceUsd: 0, count: 0 };
    g.usd += b.usd;
    g.maxUsd = Math.max(g.maxUsd, b.usd);
    g.priceUsd += b.price * b.usd;
    g.count += 1;
    groups.set(key, g);
  }
  return [...groups.values()].map((g) => ({ ...g, price: g.priceUsd / Math.max(g.usd, 1) }));
}

function updateBubbleMarkers() {
  const groups = getBubbleGroups();
  candleSeries.setMarkers(groups.sort((a, b) => a.time - b.time).map((g) => ({
    time: g.time,
    position: g.side === 'buy' ? 'belowBar' : 'aboveBar',
    color: g.side === 'buy' ? '#2ecc71' : '#e74c3c',
    shape: 'circle',
    text: `${g.side === 'buy' ? 'B' : 'S'} $${fmtUsd(g.usd)}`
  })));
  bubbleInfoEl.textContent = `Bubbles ${groups.length} / DOM $${fmtUsd(state.domThresholdUsd)}+`;
  state.markersDirty = false;
}

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
  const groups = getBubbleGroups();
  bubbleInfoEl.textContent = `Bubbles ${groups.length} / DOM $${fmtUsd(state.domThresholdUsd)}+`;

  for (const g of groups) {
    if (visible && (g.time < visible.from || g.time > visible.to)) continue;
    const x = ts.timeToCoordinate(g.time);
    const c = state.candlesByTime.get(g.time);
    const anchorPrice = c ? (g.side === 'buy' ? c.low : c.high) : g.price;
    const y = candleSeries.priceToCoordinate(anchorPrice);
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;

    // DeepChart-style soft volume bubble: aggregate all threshold trades per candle/side.
    const strength = Math.max(g.usd, g.maxUsd) / Math.max(state.domThresholdUsd, 1);
    const r = Math.min(90, 14 + Math.sqrt(strength) * 18);
    const color = g.side === 'buy' ? '46,204,113' : '231,76,60';

    const grad = ctx.createRadialGradient(x, y, 0, x, y, r);
    grad.addColorStop(0, `rgba(${color},0.58)`);
    grad.addColorStop(0.55, `rgba(${color},0.28)`);
    grad.addColorStop(1, `rgba(${color},0.02)`);
    ctx.beginPath();
    ctx.arc(x, y, r, 0, Math.PI * 2);
    ctx.fillStyle = grad;
    ctx.fill();

    ctx.beginPath();
    ctx.arc(x, y, Math.max(5, r * 0.28), 0, Math.PI * 2);
    ctx.fillStyle = `rgba(${color},0.82)`;
    ctx.fill();
    ctx.lineWidth = 2;
    ctx.strokeStyle = g.side === 'buy' ? '#2ecc71' : '#e74c3c';
    ctx.stroke();

    ctx.fillStyle = '#f4f7fb';
    ctx.font = 'bold 11px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText(`${g.side === 'buy' ? 'B' : 'S'} $${fmtUsd(g.usd)}`, x, y + 4);
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
    ctx.fillText('Live price uses book ticker while depth loads.', 12, 44);
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
$('settingsBtn').addEventListener('click', () => {
  domThresholdInputEl.value = String(state.domThresholdUsd);
  settingsModalEl.classList.remove('hidden');
});
$('settingsCancel').addEventListener('click', () => settingsModalEl.classList.add('hidden'));
$('settingsOk').addEventListener('click', () => {
  state.domThresholdUsd = Math.max(0, parseFloat(domThresholdInputEl.value) || 0);
  state.domTrades = state.bubbles.filter((b) => b.usd >= state.domThresholdUsd).slice(-80).reverse();
  state.markersDirty = true;
  updateBubbleMarkers();
  renderDomTrades();
  drawBubbles();
  settingsModalEl.classList.add('hidden');
});

chart.subscribeCrosshairMove((param) => {
  const key = timeKey(param.time);
  const candle = key ? state.candlesByTime.get(key) : state.lastCandle;
  if (!candle) {
    ohlcInfoEl.textContent = 'Move crosshair over a candle';
    return;
  }
  const delta = state.deltasByTime.get(candle.time);
  const change = candle.close - candle.open;
  const cColor = change >= 0 ? '#2ecc71' : '#e74c3c';
  const dColor = (delta || 0) >= 0 ? '#2ecc71' : '#e74c3c';
  ohlcInfoEl.innerHTML = `O <b>${fmtPrice(candle.open)}</b> H <b>${fmtPrice(candle.high)}</b> L <b>${fmtPrice(candle.low)}</b> C <b style="color:${cColor}">${fmtPrice(candle.close)}</b> Δ <b style="color:${dColor}">${Number.isFinite(delta) ? delta.toFixed(2) : '-'}</b>`;
});

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
  state.markersDirty = true;
  updateBubbleMarkers();
  drawBubbles();
});

// ---------- lifecycle ----------
async function reload() {
  setStatus('loading\u2026');
  state.orderBook = { bids: [], asks: [], mid: null };
  state.liveDeltas.clear();
  state.depthDirty = true;
  loadBubbles(); // restore persisted bubbles for this symbol
  state.domTrades = state.bubbles.filter((b) => b.usd >= state.domThresholdUsd).slice(-80).reverse();
  renderDomTrades();
  state.markersDirty = true;
  updateBubbleMarkers();
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
