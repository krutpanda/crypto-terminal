# PANDA Charting

A browser-based crypto charting platform powered by the **Binance public API**. No backend and no API key required - everything runs in the browser.

## Features

- **Spot / Futures toggle** - switch between Binance Spot and USDT-M Futures markets
- **Symbol & timeframe selection** - any Binance symbol (default `BTCUSDT`), timeframes 1m to 1d
- **Live candlestick chart** - historical klines via REST, real-time updates via WebSocket
- **Large trade bubbles** - individual big trades from the `aggTrade` stream rendered as bubbles on the chart. Bubble size scales with trade value, green = aggressive buy, red = aggressive sell. The minimum USD size is configurable in the toolbar (`Bubble >= $`)
- **Candle delta** - per-candle volume delta (taker buy volume minus taker sell volume) shown as a green/red histogram under the candles, plus the live delta of the current candle in the toolbar
- **Live depth chart** - cumulative order book (bids/asks) panel on the right, updating in real time with the mid price marked
- **Auto-center `A` button** - like TradingView: when active, the chart automatically follows the live price and keeps the latest candles centered and clearly visible. Any manual pan/zoom disables it; click `A` to re-enable

## Run

Serve the folder with any static server (WebSockets and `fetch` require an http origin):

```bash
# Python
python3 -m http.server 8080

# or Node
npx serve .
```

Then open http://localhost:8080

## Tech

- [lightweight-charts](https://github.com/tradingview/lightweight-charts) (TradingView) via CDN
- Vanilla JS + canvas overlays for bubbles and the depth chart
- Binance endpoints:
  - Spot: `api.binance.com` / `stream.binance.com`
  - Futures: `fapi.binance.com` / `fstream.binance.com`
  - Streams used: `kline_<tf>`, `aggTrade`, `depth20@100ms`
- Automatic WebSocket reconnect on disconnect