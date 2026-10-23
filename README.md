# AGR-DATA-CRYPTO

Zero-budget derivatives data collector for Binance and Bybit USDT perpetual
futures. Live collection runs on an always-on local host (Windows, Linux, or a
small VPS) because GitHub-hosted runners are geo-blocked from the exchange
APIs; GitHub Actions handles archive backfills and the daily reconciliation.
Data is stored in Supabase Postgres with a rolling five-day window. Upstash
Redis is optional and is used only for A/B ownership leases when configured.

The collector covers `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`:

| Metric | Resolution | Source |
| --- | --- | --- |
| Open interest | 5 minutes | Binance `openInterestHist`; Bybit `open-interest` |
| Long/short ratios | 5 minutes | Binance global account, top account, top position, taker volume; Bybit account ratio |
| CVD | 1 minute | Taker-classified trades: Binance `aggTrades`, Bybit `publicTrade` / archive |
| Liquidations | Event stream | Binance `!forceOrder@arr`; Bybit `allLiquidation` |

There is no OHLCV collection. CVD is built from actual taker-classified trades,
not from candle approximations.

## Deployment modes

- **Local host (current).** The two roles run on an always-on Windows PC and
  start automatically from the user's Startup folder. This is the mode that
  receives live CVD, OI, long/short, and liquidation data.
- **GitHub Actions.** `backfill` and `reconcile` run in the cloud because they
  download exchange archives rather than call the exchange REST APIs. The
  scheduled `collect-a`, `collect-b`, and `watchdog` workflows are disabled;
  enabling them on a geo-blocked runner produces no live data.
- **VPS / Oracle Cloud Always Free.** The same collector can run on a small
  always-on Linux VM for independence from the desktop. Use the Docker image or
  a Python virtualenv with a systemd unit.

The GitHub-side workflows are:

| Workflow | Status | Purpose |
| --- | --- | --- |
| `collect-a.yml`, `collect-b.yml` | disabled | former GitHub-hosted live collectors |
| `watchdog.yml` | disabled | former collector dispatcher |
| `backfill.yml` | manual | initial five-day load or targeted repair |
| `reconcile.yml` | daily, 04:20 UTC | rebuild the previous UTC day from archives, prune, verify |
| `keepalive.yml` | monthly | keep scheduled workflows from being disabled |
| `ci.yml` | push / PR | tests, lint, and workflow checks |

The collector workflows are disabled because GitHub-hosted runners cannot reach
the exchange APIs from their network. `reconcile` remains active: it only
downloads public archives and talks to Supabase.

## Architecture

```
Local host / VPS                          Supabase Postgres
┌──────────────────────────────┐          ┌──────────────────────────┐
│ alpha: Binance REST + WS     │ upserts  │ deriv_open_interest      │
│ bravo: Bybit WS              │─────────▶│ deriv_long_short         │
└──────────────────────────────┘          │ deriv_cvd_1m             │
                                          │ deriv_liquidations       │
GitHub Actions (archive jobs)             │ + 5-minute views         │
┌──────────────────────────────┐          │                          │
│ backfill / reconcile         │─────────▶│                          │
└──────────────────────────────┘          └──────────────────────────┘
```

- `alpha` collects Binance: OI, long/short, CVD, and the liquidation feed.
- `bravo` collects Bybit: OI, long/short, CVD, and liquidations.
- Both roles poll OI and long/short idempotently. Duplicate writes are
  harmless because every table has an exchange/symbol/timestamp primary key.
- With Redis configured, the roles use leases and heartbeats for failover.
  Without Redis (the current local deployment), each role runs only its
  primary exchange's live streams.
- The writer pipeline deduplicates rows in memory, flushes in batches, and
  retries a failed batch on the next flush.

### Ownership and shutdown

The live roles are ordinary long-running processes. On Windows, two hidden
PowerShell wrappers start one process per role and restart it after a crash;
a stop flag allows a clean shutdown. Logs are rotated daily. On a VPS, use
systemd units or a process supervisor with the same restart semantics.

Upstash Redis is optional. When it is configured, each role publishes a
heartbeat and takes a lease for the streams it owns; the peer can take over a
missing exchange after the lease expires. The lease is fenced with a Redis
`INCR` value, so a stale owner cannot overwrite a newer owner. When Redis is
not configured, alpha owns Binance and bravo owns Bybit, and neither role
attempts a takeover.

Do not run two copies of the same role against the same Redis key space. With
Redis disabled, do not start a second alpha/bravo pair on the same host: the
duplicate streams are idempotently written, but they waste exchange rate limit
and can create misleading gaps during restarts.

## Data model

Apply [`sql/schema.sql`](sql/schema.sql) in the Supabase SQL editor.

### `deriv_open_interest`

Primary key: `(exchange, symbol, ts)`.

- `open_interest` — base-asset open interest.
- `open_interest_value` — USDT notional where the exchange supplies it.
- Bybit currently omits `openInterestValue`; the column is nullable.

### `deriv_long_short`

Primary key: `(exchange, symbol, kind, ts)`.

`kind` values:

- Binance: `global_account`, `top_account`, `top_position`, `taker_volume`.
- Bybit: `account`.

`unit` is `fraction` for account/position ratios and `base_volume` for the
Binance taker buy/sell volume series. For `taker_volume`, `long_value` is taker
buy volume, `short_value` is taker sell volume, and `ratio` is buy/sell.

### `deriv_cvd_1m`

Primary key: `(exchange, symbol, ts)`.

- `buy_volume`, `sell_volume` — taker-classified base-asset volume.
- `buy_notional`, `sell_notional` — price times quantity.
- `trades` — number of trades in the bucket.
- `delta` — generated by Postgres as `buy_volume - sell_volume`.
- `source` — `live` or `archive`.

Live rows are written when a minute closes. Empty minutes are normally filled
by archive reconciliation the next day. A live minute with no trades therefore
does not necessarily have a zero row until reconciliation.

### `deriv_liquidations`

Primary key: `(exchange, event_id)`.

- `order_side` — the exchange order side (`buy` or `sell`).
- `position_side` — inferred position side: a sell liquidation closes a long,
  a buy liquidation closes a short.
- `notional` — price times quantity.
- `source` — currently `live`.

Views:

- `deriv_cvd_5m` — 5-minute aggregation of CVD.
- `deriv_cvd_cumulative` — running delta per exchange/symbol.
- `deriv_liquidation_1m` — liquidation volume by minute and position side.

Retention is five days. `reconcile.yml` calls `prune_derivatives(5)` daily.
The same function can be called manually:

```sql
select public.prune_derivatives(5);
```

## Exactness and known limits

- Binance `aggTrades` uses the `m` flag to determine the taker side. Bybit
  `publicTrade` and the public archive expose the taker side directly.
- Bybit's live CVD stream starts the WebSocket first, buffers trades while it
  warms up from REST, then aggregates the still-open minute from both sources.
  Binance, when `COLLECTOR_CVD_REST_EXCHANGES=binance` is set, seeds from
  `aggTrades` and then polls the just-closed minute over REST.
- Binance REST recent trades stop after 30,000 trades. Bybit REST recent trades
  return at most 1,000 trades. A warmup longer than those pages can leave a
  current-day gap; the next daily archive reconciliation replaces the entire
  previous UTC day and repairs it. The Binance seed is chunked into 10-minute
  windows and paced at 0.6 seconds per page, so a long catch-up stays under the
  exchange weight limit.
- Daily reconciliation downloads Binance Vision `aggTrades` and Bybit public
  trade archives, rebuilds every minute including zero-volume minutes, and
  overwrites existing CVD rows with `source = archive`.
- Free historical liquidation data is not available. Liquidations are collected
  only from deployment onward. Exchange public liquidation feeds are known to be
  incomplete, so these rows are an observed-event sample, not a complete
  historical record.
- Bybit OI does not provide a notional value in the current endpoint, so
  `open_interest_value` is `null` for Bybit.
- Bybit's recent-trades endpoint returns at most 1,000 trades, which may cover
  less than one minute for BTCUSDT. Its live warmup therefore usually starts at
  the current minute, not ten minutes back. A restart can leave a short
  current-day Bybit CVD gap; the next daily archive reconciliation repairs it.
- The local wrappers restart a crashed collector after a short delay, but a
  restart still loses the seconds between the crash and the new process. The
  next daily archive reconciliation rebuilds the previous UTC day and repairs
  those gaps.
- Binance's trade WebSocket data plane is silent from some networks. The REST
  fallback (`COLLECTOR_CVD_REST_EXCHANGES=binance`) is the supported workaround.
  Binance liquidations have no usable public REST history, so that table stays
  empty for Binance in this mode.

## Setup

### 1. Create Supabase

1. Create a Supabase project.
2. Open the SQL editor and run `sql/schema.sql`.
3. Copy the project URL and the `service_role` key. Keep the service-role key
   private: it bypasses RLS.

### 2. (Optional) Create Upstash Redis

Redis is not required for the current local deployment. Skip this step unless
you want automatic A/B failover between two roles.

1. Create a free Upstash Redis database.
2. Copy the TLS connection URL (`rediss://...`).
3. No keys or streams need to be created manually.

### 3. Configure GitHub Secrets

In the repository settings, add:

| Secret | Required | Value |
| --- | --- | --- |
| `SUPABASE_URL` | yes | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | yes | Supabase service-role key |
| `UPSTASH_REDIS_URL` | no | Upstash TLS Redis URL (optional; enables A/B leases) |
| `TELEGRAM_BOT_TOKEN` | no | Telegram bot token for failure notices |
| `TELEGRAM_CHAT_ID` | no | Telegram chat ID for failure notices |

Never put these values in a tracked file or commit message. The application
uses public Binance/Bybit market-data endpoints, so exchange API keys are not
required. The two Supabase secrets are required for the archive workflows.
Redis is optional. For local live collection, keep the same values in a
private environment file; `.env.local` is gitignored.

### 4. Initial backfill

Open **Actions → backfill → Run workflow** with:

- exchange: `both`
- symbols: `BTCUSDT,ETHUSDT,SOLUSDT`
- days: `5`
- include: `oi,ls,cvd`
- replace_cvd: `true`
- prune: `false`

The first archive backfill downloads large daily files and can take a while.
Liquidations cannot be backfilled.

### 5. Start collection

The current deployment starts both roles locally. Set the environment from a
private file and launch one process per role; the repository's `.env.example`
lists the supported variables. A minimal pair is:

```powershell
$env:PYTHONPATH = "src"
$env:COLLECTOR_ROLE = "alpha"   # use "bravo" for the Bybit process
$env:COLLECTOR_SYMBOLS = "BTCUSDT,ETHUSDT,SOLUSDT"
$env:COLLECTOR_SUPABASE_URL = "https://your-project.supabase.co"
$env:COLLECTOR_SUPABASE_KEY = "service-role-key"
.\.venv\Scripts\python.exe -m collector.main
```

Run `alpha` and `bravo` as separate, supervised processes. On Windows, place a
shortcut to the start script in `shell:startup`. On Linux, use two systemd
units with `Restart=always`. Do not commit the environment file or the
service-role key.

For the Binance CVD workaround used on networks where the trade WebSocket is
silent, set `COLLECTOR_CVD_REST_EXCHANGES=binance`. The collector then seeds and
polls `aggTrades` over REST, paced at 0.6 seconds per page. This setting is
local-only; it is not required for the archive workflows.

Keep the host awake and connected. The two roles are independent processes:
alpha owns Binance and bravo owns Bybit when Redis is not configured. The
Windows wrappers in the current deployment live outside the repository; they
clear a stop flag on start, set it on stop, rotate logs daily, and restart a
crashed process. On Linux, use two systemd units with `Restart=always`.

### 6. Verify

After collection has run for a few minutes:

```powershell
$env:PYTHONPATH = "src"
$env:COLLECTOR_SUPABASE_URL = "https://your-project.supabase.co"
$env:COLLECTOR_SUPABASE_KEY = "service-role-key"

# Live continuity: newest timestamp, row count, and largest gap.
python scripts/verify_continuity.py --window-minutes 260

# Rebuilt previous UTC day: exact 288 five-minute points per metric and 1440
# one-minute CVD buckets, with exact first/last boundaries and no gaps.
python scripts/verify_continuity.py `
  --start 2026-10-20T00:00:00Z `
  --end 2026-10-21T00:00:00Z
```

The live mode tolerates a recently started collector by allowing two missing
points at the start of the window; the historical mode is exact and fails on a
single missing or misplaced bucket. Both modes use keyset pagination, so a
24-hour CVD check is not truncated by the PostgREST page limit. A successful
daily reconciliation runs both checks automatically.

## Local development

Requirements: Python 3.12.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check .
python scripts/check_workflows.py
```

The application reads process environment variables; it does not load `.env`.
Use [`.env.example`](.env.example) as a reference and set the values in your
shell or CI configuration.

### Dry run

A dry run uses an in-memory sink and does not require Supabase. Redis is also
optional: without it, each role runs only its primary exchange.

```powershell
$env:PYTHONPATH = "src"
$env:COLLECTOR_DRY_RUN = "true"
$env:COLLECTOR_ROLE = "alpha"
$env:COLLECTOR_SYMBOLS = "BTCUSDT"
$env:COLLECTOR_SOFT_DEADLINE_SEC = "30"
.\.venv\Scripts\python.exe -m collector.main
```

Run the same command with `COLLECTOR_ROLE=bravo` to exercise Bybit.

### Backfill and reconciliation

```powershell
# Five rolling days into Supabase.
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe scripts\backfill_derivatives.py `
  --exchange both `
  --symbols BTCUSDT,ETHUSDT,SOLUSDT `
  --days 5 `
  --include oi,ls,cvd `
  --replace-cvd

# Rebuild one UTC day.
.\.venv\Scripts\python.exe scripts\backfill_derivatives.py `
  --exchange both `
  --symbols BTCUSDT `
  --start 2026-10-20T00:00:00Z `
  --end 2026-10-21T00:00:00Z `
  --include oi,ls,cvd `
  --replace-cvd

# Dry-run the archive calculations without writing to Supabase.
.\.venv\Scripts\python.exe scripts\backfill_derivatives.py `
  --exchange both `
  --symbols BTCUSDT `
  --start 2026-10-20T00:00:00Z `
  --end 2026-10-21T00:00:00Z `
  --dry-run
```

### Docker

The Docker image is a portable single-role runner. Run one container per role
when moving live collection to a VPS or an Oracle Cloud Always Free instance.

```powershell
docker build -t agr-data-crypto -f docker/Dockerfile .
docker run --env-file .env agr-data-crypto
```

The image defaults to `COLLECTOR_ROLE=alpha`; set `COLLECTOR_ROLE=bravo` for the
second container. Both containers should use the same Redis URL.

## Cost

The intended deployment is zero-budget:

- **GitHub Actions:** standard runners are free for a public repository. The
  30-minute handoff overlap means each role runs about 27 hours per day, or
  roughly 54 runner-hours for the pair. A private repository consumes the
  account's included Actions minutes, which this design will exhaust quickly;
  use a public repository for the zero-budget target.
- **Supabase Free:** the five-day window is roughly tens of thousands of rows,
  well below the free database limit.
- **Upstash Free:** heartbeats and takeover polls are intentionally paced to
  stay within the daily command quota. Watchdog and reconciliation add only a
  small number of commands.

Archive backfills use GitHub runner bandwidth and disk, not paid storage.

## Repository layout

```
src/collector/
  config.py           environment settings and role mapping
  models.py           derivative rows and table names
  aggregation.py      exact CVD bucketing
  streams.py          metrics poller, CVD stream, liquidation stream
  supervisor.py       A/B leases, heartbeats, takeover
  pipeline.py         deduplicating write buffer
  exchanges/          REST/WS adapters and archive parsers
  sink/               Supabase and memory sinks
  state/              Redis lease manager
scripts/
  backfill_derivatives.py
  verify_continuity.py
  watchdog.py
  check_workflows.py
sql/schema.sql
.github/workflows/
tests/
```

## MCP server (OpenCode)

The repository ships a read-only MCP server that exposes the collected
dataset to OpenCode (and any other MCP client) over stdio:

- Tools: `health`, `get_open_interest`, `get_long_short`, `get_cvd`,
  `get_liquidations`, `get_cvd_cumulative`, `get_cvd_5m`,
  `get_liquidation_1m`.
- Timestamps are milliseconds since epoch (UTC); `start`/`end` accept ms
  epoch or ISO-8601.

Install the optional dependency and run it:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[mcp]"
$env:PYTHONPATH = "src"
$env:COLLECTOR_SUPABASE_URL = "https://your-project.supabase.co"
$env:COLLECTOR_SUPABASE_KEY = "service-role-key"
.\.venv\Scripts\python.exe -m collector.mcp_server
```

Project-local OpenCode configuration is already in `opencode.json`. Set the
Supabase credentials in your shell (or via `{env:...}` substitution) and the
server connects automatically when OpenCode starts.

## Security

- No exchange API keys are used.
- Supabase service-role credentials and Upstash URLs must stay in GitHub
  Secrets or a local, untracked environment.
- The schema revokes table and view access from `anon` and `authenticated`;
  only `service_role` can read or write.
- `.env` is ignored by Git. Never commit a populated environment file.
- Do not commit local strategy code or other proprietary trading logic to this
  repository.

## Verification checklist

Before leaving a deployment unattended:

1. `ci.yml` is green.
2. Supabase contains rows in all four tables.
3. `verify_continuity.py --window-minutes 260` exits with code 0.
4. `collect-a` and `collect-b` both show heartbeats in Upstash.
5. The watchdog can dispatch a missing collector.
6. The next daily reconciliation completes, leaves `source = archive` rows for
   the previous UTC day, and `verify_continuity.py --start <yesterday>
   --end <today>` exits with code 0.
