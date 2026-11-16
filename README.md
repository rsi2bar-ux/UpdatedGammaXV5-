# GAMMAX V5.2 PRO — Persistent One-Second Market Recorder

Derived from the supplied GammaXBinery-2.zip / Bharati Unique Backend V4. Files are at ZIP root (no nested project folder).

## Features
- TOTP-based automatic access-token generation and proactive refresh; no manual daily access-token paste.
- Live Dhan market feed and automatic reconnect.
- Correct 20-level and 200-level depth binary packet decoders.
- 200-level depth uses a dedicated one-instrument connection as required by Dhan.
- Option chain, Dhan Greeks, IV, OI, PCR, Max Pain, OI walls and model-derived hidden Greeks.
- Market Quote snapshots.
- Persistent one-second market snapshots with futures depth, option-chain OI, Change in OI, volume and Change in Volume.
- Historical intraday/daily and rolling expired-option endpoints.
- Technical Data, Market Movers, News Headlines APIs.
- Instrument master, REST, WebSocket, push notifications.
- Trading execution remains disabled.

## Deploy on Render
1. Upload these flat files to GitHub.
2. Create a Node Web Service. Build: `npm install`; Start: `npm start`.
3. Copy `.env.example` values into Render Environment. Set Dhan Client ID, PIN and TOTP secret.
4. Set CORS_ORIGIN to your deployed PWA domain(s). Use `*` only for initial testing.
5. Deploy and verify `/api/health`. Confirm `snapshots.ready=true`, then confirm `futuresPinned` is 2 when NIFTY and SENSEX futures are found.
6. In Render, verify a persistent disk is attached at `/var/data`. If this is an existing manually-created service and the Blueprint disk is not synced, open the service **Disks** page and add a 10 GB disk mounted at `/var/data`.

## REST endpoints
Existing V4: `/api/health`, `/api/config`, `/api/state`, `/api/tick`, `/api/ticks`, `/api/option-chain`, `/api/analytics`, `/api/history`, `/api/instruments`, `/api/depth`, `/api/index`, `/api/expiry`, `/api/expiry/select`, `/api/subscribe`, `/api/push/subscribe`, `/api/push/test`.
Added: `POST /api/depth/20/subscribe` to pin selected instruments (such as the active index future) to the 20-level depth socket across reconnects; `GET /api/market-quote`, `POST /api/market-quote`, `POST /api/depth/200/subscribe`, `GET /api/depth?...&levels=200`, `POST /api/history/intraday`, `/api/history/daily`, `/api/history/expired-options`, `/api/technical`, `/api/market-movers`, `/api/news`, `/api/company-info` (disabled by default), `POST /api/auth/refresh`; snapshot recorder: `GET /api/snapshots/status`, `GET /api/snapshots/dates`, `GET /api/snapshots?date=YYYY-MM-DD&from=<epoch-ms>&to=<epoch-ms>&index=NIFTY&limit=1000`.
WebSocket `/ws` broadcasts `hello`, `state`, `optionChain`, `tick`, `depth`, `depth200Status`, `error`.

## Dhan plan features
The screenshot shows 200-level depth, Technical Data, Market Movers, CAS Live Feed, News Headlines and Company Fundamentals (Coming Soon). Public DhanHQ docs verify `/data/technical`, `/data/marketmovers`, `/data/newsheadline`, `/marketfeed/quote`, `/charts/intraday`, `/charts/historical`, `/charts/rollingoption`, and 200-depth websocket. CAS Live Feed is not documented in the supplied V4 source or the public API contract verified for this build, so it is explicitly disabled rather than fabricated. Company Info endpoint exists in current API references but is disabled by default pending confirmation that Dhan has enabled it for your account.

## 200-depth limitation
Dhan's 200-depth WebSocket accepts one instrument per connection. V5 supports one selected 200-level contract at a time, not 200 levels for every option strike simultaneously. The 20-depth socket can subscribe multiple instruments.

## Authentication
Dhan access tokens have 24-hour validity. TOTP-enabled accounts can generate access tokens programmatically. V5 stores credentials server-side and refreshes before expiry. If TOTP is rejected, check server clock synchronization, client ID, PIN and TOTP secret.

## Security
Never commit `.env`. Do not place Dhan credentials or tokens in frontend code. Restrict CORS in production. The VAPID private key shown in your message should be rotated; use a newly generated pair. Trading API and live order execution are disabled by default.


## V5.2 persistent market snapshots
- Captures one record per second during weekdays in the Indian cash/F&O session (09:15–15:30 Asia/Kolkata). Exchange holidays are not separately calendar-filtered.
- Records active index, spot, pinned NIFTY/SENSEX futures ticks, 20-level futures depth, bid/ask depth-side timestamps, and 21 strikes (10 ITM + ATM + 10 OTM) of CE/PE option-chain LTP, OI, Change in OI, volume and Change in Volume.
- Each option field carries its server-receive timestamp and source label; LTP rows also preserve the Dhan trade timestamp when the tick packet provides one. Depth timestamps are local packet-receive times because the decoded depth packet does not include an exchange event timestamp. Option-chain data is refreshed at the existing 'OPTION_CHAIN_REFRESH_MS' cadence (default 3200 ms); the one-second recorder does not call Dhan's option-chain REST API every second or pretend unchanged values are new updates.
- Daily JSONL files are written under `SNAPSHOT_DIR`, compressed after the day closes, and pruned after `SNAPSHOT_RETENTION_DAYS`.
- REST: `GET /api/snapshots/status`, `GET /api/snapshots/dates`, `GET /api/snapshots?date=YYYY-MM-DD&from=<epoch-ms>&to=<epoch-ms>&index=NIFTY&limit=1000`.
- Render Blueprint attaches a 10 GB persistent disk at '/var/data'. Render's filesystem is otherwise ephemeral. If you deploy the service without syncing the disk blueprint, attach a persistent disk manually at '/var/data' before relying on stored history.
- The app reports `persistentDiskExpected`, not `persistentDiskVerified`: it cannot prove that a Render disk is attached merely from its filesystem path. Verify the Disk page in Render.
- Disk-backed web services are single-instance; do not scale this recorder to multiple instances sharing the same local JSONL path.


## 30-second OHLC extension (OI Sniper build)
- The tick recorder now aggregates live Dhan ticks into 30s, 1m, 3m, 5m, 15m, 30m and 60m OHLC series in memory.
- `GET /api/history?segment=NSE_FNO&securityId=<future-security-id>&tf=30s` returns 30-second bars.
- `GET /api/candles?segment=NSE_FNO&securityId=<future-security-id>&tf=30s&limit=2000` is the equivalent candles endpoint.
- If fewer than 50 live 30-second bars are present, the backend reconstructs earlier 30-second bars from its persistent one-second snapshot recorder, when those snapshots exist. Reconstructed bars are labelled `PERSISTENT_1S_SNAPSHOTS`; they are not exchange-native historical 30-second candles.
- Dhan's documented intraday chart API does not provide a native 30-second interval in this implementation. Exact 30-second OHLC is therefore built from received live ticks; earlier bars can only be reconstructed from the saved one-second snapshots.
- Set `CANDLE_TIMEFRAME_MS=30000`. The `SUPPORTED_TIMEFRAMES` list must include `30000`. These variables configure backend aggregation; they do not create 30-second bars from historical 1-minute data.

## Signal push notification
- Registered PWA subscriptions receive a server push when the live page confirms a new entry and posts it to `/api/push/signal`. This is a notification path, not independent backend-side strategy calculation: the PWA must be running to detect the entry. The in-page three-pulse alarm also requires the PWA to be foregrounded and sound enabled.


## Active/standby failover (V5.2 primary)

- Set `FAILOVER_STANDBY=false` on the V5.2 primary.
- The V6.1 backup should have `FAILOVER_STANDBY=true`.
- The PWA activates the backup through `POST /api/failover/activate` only when the primary API/auth/feed fails during market hours.
- The PWA places the old backend into standby through `POST /api/failover/standby` after a successful switch.
- `/api/health` now exposes authentication state, socket readiness, packet counters, last packet/tick timestamps, subscription diagnostics, decoder errors and feed status.
- `POST /api/failover/activate` and `/api/failover/standby` are intended for your private PWA/backend pair. Do not expose these controls to untrusted clients.
- Do not run both feeds active by default. Dhan documents a maximum of five market-feed WebSocket connections per client ID.

Both deployments must have the same Dhan account credentials configured as Render environment secrets (`DHAN_CLIENT_ID`, `DHAN_PIN`, `DHAN_TOTP_SECRET`) and the required API settings. The backup should remain in standby and will generate/refresh its own access token only after activation. Never put these credentials in the PWA or commit them to Git.
