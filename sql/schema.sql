-- Derivatives collector schema. Run in the Supabase SQL editor.
-- Live collection writes closed 1m/5m buckets; daily reconciliation overwrites
-- archive-derived CVD so gaps and duplicates self-heal.

create table if not exists public.deriv_open_interest (
  exchange text not null check (exchange in ('binance', 'bybit')),
  symbol text not null,
  ts bigint not null,
  open_interest double precision not null,
  open_interest_value double precision,
  received_at timestamptz not null default now(),
  primary key (exchange, symbol, ts)
);

create table if not exists public.deriv_long_short (
  exchange text not null check (exchange in ('binance', 'bybit')),
  symbol text not null,
  kind text not null,
  ts bigint not null,
  long_value double precision not null,
  short_value double precision not null,
  ratio double precision,
  unit text not null check (unit in ('fraction', 'base_volume')),
  received_at timestamptz not null default now(),
  primary key (exchange, symbol, kind, ts)
);

create table if not exists public.deriv_cvd_1m (
  exchange text not null check (exchange in ('binance', 'bybit')),
  symbol text not null,
  ts bigint not null,
  buy_volume double precision not null default 0,
  sell_volume double precision not null default 0,
  buy_notional double precision not null default 0,
  sell_notional double precision not null default 0,
  trades integer not null default 0,
  delta double precision generated always as (buy_volume - sell_volume) stored,
  source text not null default 'live' check (source in ('live', 'archive')),
  received_at timestamptz not null default now(),
  primary key (exchange, symbol, ts)
);

create table if not exists public.deriv_liquidations (
  exchange text not null check (exchange in ('binance', 'bybit')),
  event_id text not null,
  symbol text not null,
  ts bigint not null,
  order_side text not null check (order_side in ('buy', 'sell')),
  position_side text not null check (position_side in ('long', 'short')),
  price double precision not null,
  qty double precision not null,
  notional double precision not null,
  source text not null default 'live',
  received_at timestamptz not null default now(),
  primary key (exchange, event_id)
);

create index if not exists deriv_open_interest_ts_idx on public.deriv_open_interest (ts);
create index if not exists deriv_long_short_ts_idx on public.deriv_long_short (ts);
create index if not exists deriv_cvd_1m_ts_idx on public.deriv_cvd_1m (ts);
create index if not exists deriv_liquidations_ts_idx on public.deriv_liquidations (ts);
create index if not exists deriv_liquidations_symbol_ts_idx on public.deriv_liquidations (symbol, ts);

create or replace view public.deriv_cvd_5m
with (security_invoker = true) as
select
  exchange,
  symbol,
  ts - (ts % 300000) as ts,
  sum(buy_volume) as buy_volume,
  sum(sell_volume) as sell_volume,
  sum(buy_notional) as buy_notional,
  sum(sell_notional) as sell_notional,
  sum(trades) as trades,
  sum(delta) as delta
from public.deriv_cvd_1m
group by exchange, symbol, ts - (ts % 300000);

create or replace view public.deriv_cvd_cumulative
with (security_invoker = true) as
select
  exchange,
  symbol,
  ts,
  buy_volume,
  sell_volume,
  delta,
  sum(delta) over (
    partition by exchange, symbol
    order by ts
    rows between unbounded preceding and current row
  ) as cumulative_delta
from public.deriv_cvd_1m;

create or replace view public.deriv_liquidation_1m
with (security_invoker = true) as
select
  exchange,
  symbol,
  ts - (ts % 60000) as ts,
  position_side,
  sum(qty) as qty,
  sum(notional) as notional,
  count(*)::integer as events
from public.deriv_liquidations
group by exchange, symbol, ts - (ts % 60000), position_side;

create or replace function public.prune_derivatives(
  p_days integer default 5,
  p_now timestamptz default now()
)
returns void
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  cutoff bigint;
begin
  cutoff := (extract(epoch from (p_now - make_interval(days => p_days))) * 1000)::bigint;
  delete from public.deriv_open_interest where ts < cutoff;
  delete from public.deriv_long_short where ts < cutoff;
  delete from public.deriv_cvd_1m where ts < cutoff;
  delete from public.deriv_liquidations where ts < cutoff;
end;
$$;

alter table public.deriv_open_interest enable row level security;
alter table public.deriv_long_short enable row level security;
alter table public.deriv_cvd_1m enable row level security;
alter table public.deriv_liquidations enable row level security;

revoke all on public.deriv_open_interest from anon, authenticated;
revoke all on public.deriv_long_short from anon, authenticated;
revoke all on public.deriv_cvd_1m from anon, authenticated;
revoke all on public.deriv_liquidations from anon, authenticated;
revoke all on public.deriv_cvd_5m from anon, authenticated;
revoke all on public.deriv_cvd_cumulative from anon, authenticated;
revoke all on public.deriv_liquidation_1m from anon, authenticated;

grant all on public.deriv_open_interest to service_role;
grant all on public.deriv_long_short to service_role;
grant all on public.deriv_cvd_1m to service_role;
grant all on public.deriv_liquidations to service_role;
grant select on public.deriv_cvd_5m to service_role;
grant select on public.deriv_cvd_cumulative to service_role;
grant select on public.deriv_liquidation_1m to service_role;

revoke all on function public.prune_derivatives(integer, timestamptz) from public, anon, authenticated;
grant execute on function public.prune_derivatives(integer, timestamptz) to service_role;

comment on table public.deriv_liquidations is
  'Live-only; exchange public liquidation feeds are intentionally incomplete.';
