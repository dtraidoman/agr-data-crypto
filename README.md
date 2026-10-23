# AGR-DATA-CRYPTO

Zero-budget derivatives data collector for Binance and Bybit USDT perpetual
futures. It runs as an A/B pair of GitHub Actions jobs, stores a rolling
five-day window in Supabase Postgres, and uses Upstash Redis only for ownership
leases and heartbeats.

The collector covers `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`:

| Metric | Resolution | Source |
| --- | --- | --- |
| Open interest | 5 minutes | Binance `openInterestHist`; Bybit `open-interest` |
| Long/short ratios | 5 minutes | Binance global account, top account, top position, taker volume; Bybit account ratio |
| CVD | 1 minute | Taker-classified trades: Binance `aggTrades`, Bybit `publicTrade` / archive |
| Liquidations | Event stream | Binance `!forceOrder@arr`; Bybit `allLiquidation` |

There is no OHLCV collection. CVD is built from actual taker-classified trades,
not from candle approximations.

## Architecture

```
GitHub Actions                         Upstash Redis
┌─────────────────────┐               ┌──────────────────────┐
│ collect-a (alpha)   │  leases /     │ hb:alpha             │
│ primary: Binance    │  heartbeats   │ hb:bravo             │
│ secondary: Bybit    │◄─────────────►│ lease:cvd:binance    │
└─────────┬───────────┘               │ lease:cvd:bybit      │
          │                           │ lease:liq:*          │
          │                           └──────────────────────┘
          │ idempotent upserts
          ▼
┌─────────────────────┐               ┌──────────────────────┐
│ collect-b (bravo)   │               │ Supabase Postgres    │
│ primary: Bybit      │──────────────►│ four derivative      │
│ secondary: Binance  │               │ tables + views       │
└─────────────────────┘               └──────────────────────┘
```

- `alpha` owns the Binance CVD and liquidation WebSockets.
- `bravo` owns the Bybit CVD and liquidation WebSockets.
- Both roles poll OI and long/short data idempotently. Duplicate writes are
  harmless because every table has an exchange/symbol/timestamp primary key.
- If the peer heartbeat disappears, the surviving role can take over the
  missing exchange. When Redis is unavailable, each role still collects its
  primary exchange, so the pair continues to cover both exchanges.
- Leases are fenced with a Redis `INCR` value. A stale owner cannot overwrite a
  newer owner's lease.
- Heartbeat interval is 60 seconds and lease TTL is 180 seconds. The 60-second
  takeover poll keeps normal Upstash usage below the free command quota.
- The writer pipeline deduplicates rows in memory, flushes in batches, and
  retries a failed batch on the next flush.

### Ownership and shutdown

Each collector job has a soft deadline of 4 hours 30 minutes (`16,200` seconds)
and a GitHub hard timeout of 350 minutes. The schedules repeat every four hours,
so the successor starts 30 minutes before its predecessor exits and waits on the
exchange stream leases. The predecessor keeps streaming until its deadline and
then releases the leases; the successor acquires them on its next 60-second
poll. This deliberate overlap absorbs GitHub schedule jitter and avoids a
10-minute hole at every handoff. Primary-key upserts make the brief overlap
safe.

Do not add a GitHub `concurrency` group that queues the successor behind the
predecessor: that turns the overlap into an outage. The Redis leases, not the
workflow queue, arbitrate ownership.

The schedules are:

| Workflow | Schedule (UTC) | Purpose |
| --- | --- | --- |
| `collect-a.yml` | `0 */4 * * *` | alpha, primary Binance |
| `collect-b.yml` | `10 */4 * * *` | bravo, primary Bybit |
| `watchdog.yml` | `*/15 * * * *` | dispatch a collector when its heartbeat is missing |
| `reconcile.yml` | `20 4 * * *` | rebuild the previous UTC day from archives, prune, verify the rebuilt day and live continuity |
| `keepalive.yml` | monthly | prevent GitHub from disabling scheduled workflows |
| `backfill.yml` | manual | initial five-day load or targeted repair |
| `ci.yml` | push / PR | tests, lint, and workflow checks |

If both collector heartbeats are absent, the watchdog dispatches both
collectors instead of waiting for manual intervention. It needs the repository
`GITHUB_TOKEN` with permission to dispatch workflows.

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
- The live CVD stream starts the WebSocket first, buffers trades while it warms
  up from REST, then aggregates the still-open minute from both REST and the
  buffered stream. Completed minutes are persisted by the normal flush.
- Binance REST recent trades stop after 30,000 trades. Bybit REST recent trades
  return at most 1,000 trades. A warmup longer than those pages can leave a
  current-day gap; the next daily archive reconciliation replaces the entire
  previous UTC day and repairs it.
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
  the current minute, not ten minutes back. Normal A/B handoffs are covered by
  the 30-minute job overlap; a longer unexpected outage leaves a current-day
  Bybit CVD gap that the next daily archive reconciliation repairs.
- GitHub Actions scheduled jobs can start late. The watchdog exists to reduce
  the resulting gaps; the A/B overlap and next-day reconciliation provide the
  second layer of recovery.

## Setup

### 1. Create Supabase

1. Create a Supabase project.
2. Open the SQL editor and run `sql/schema.sql`.
3. Copy the project URL and the `service_role` key. Keep the service-role key
   private: it bypasses RLS.

### 2. Create Upstash Redis

1. Create a free Upstash Redis database.
2. Copy the TLS connection URL (`rediss://...`).
3. No keys or streams need to be created manually.

### 3. Configure GitHub Secrets

In the repository settings, add:

| Secret | Required | Value |
| --- | --- | --- |
| `SUPABASE_URL` | yes | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | yes | Supabase service-role key |
| `UPSTASH_REDIS_URL` | yes | Upstash TLS Redis URL |
| `TELEGRAM_BOT_TOKEN` | no | Telegram bot token for failure notices |
| `TELEGRAM_CHAT_ID` | no | Telegram chat ID for failure notices |

Never put these values in a tracked file or commit message. The application
uses public Binance/Bybit market-data endpoints, so exchange API keys are not
required.

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

Enable the scheduled workflows (`collect-a`, `collect-b`, `watchdog`,
`reconcile`) and run `collect-a` and `collect-b` once manually if you do not
want to wait for the next cron tick. A/B ownership is established through
Upstash.

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
when a VPS or Oracle Cloud Always Free instance replaces GitHub Actions.

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
