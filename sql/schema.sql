-- Run once in the Supabase SQL editor.
--
-- Source of truth for collection is 1m; higher timeframes are derived on read
-- (see ohlcv_agg). This keeps the idempotency surface at exactly one key per
-- minute per stream.

create table if not exists public.ohlcv (
    exchange     text    not null check (exchange in ('binance', 'bybit')),
    symbol       text    not null,
    interval     text    not null default '1m',
    open_time    bigint  not null,          -- exchange-provided, UTC epoch ms
    close_time   bigint  not null,
    open         double precision not null,
    high         double precision not null,
    low          double precision not null,
    close        double precision not null,
    volume       double precision not null,
    quote_volume double precision,
    trades       integer,
    is_closed    boolean not null default true,
    received_at  timestamptz not null default now(),
    constraint ohlcv_pk primary key (exchange, symbol, interval, open_time)
);

-- Supports "latest N" and "since T" scans per stream. The PK index already
-- covers equality on (exchange, symbol, interval) + range on open_time
-- ascending; a descending index is only worth it above ~50M rows.
comment on table public.ohlcv is
    'Closed candles only. Never write is_closed=false rows: they would stay partial forever.';

-- Rollup helper: 1m -> 1h, without a second idempotency surface.
create or replace view public.ohlcv_agg
with (security_invoker = true) as
select
    exchange,
    symbol,
    date_trunc('hour', to_timestamp(open_time / 1000.0)) as bucket,
    (array_agg(open  order by open_time))[1]  as open,
    max(high)                                 as high,
    min(low)                                  as low,
    (array_agg(close order by open_time desc))[1] as close,
    sum(volume)                               as volume,
    count(*)                                  as minutes
from public.ohlcv
where interval = '1m' and is_closed
group by exchange, symbol, bucket;

-- Lock the table down: only the service_role key (used by Actions) can touch it.
alter table public.ohlcv enable row level security;
revoke all on public.ohlcv from anon, authenticated;

-- Operational sanity check, callable from scripts/verify_continuity.py as RPC
-- if you prefer server-side gap detection.
create or replace function public.ohlcv_gaps(
    p_exchange text,
    p_symbol   text,
    p_from_ms  bigint,
    p_to_ms    bigint
) returns table (gap_start bigint, gap_minutes bigint)
language sql stable as $$
    with expected as (
        select generate_series(p_from_ms, p_to_ms - 60000, 60000) as t
    )
    select e.t, 1
    from expected e
    left join public.ohlcv o
      on o.exchange = p_exchange
     and o.symbol = p_symbol
     and o.interval = '1m'
     and o.open_time = e.t
    where o.open_time is null;
$$;

revoke all on function public.ohlcv_gaps(text, text, bigint, bigint) from anon, authenticated;
