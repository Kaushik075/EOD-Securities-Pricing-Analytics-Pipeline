USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;
CREATE SCHEMA IF NOT EXISTS SA;
USE SCHEMA SA;



-- Load Internal data (Focusing on Security and ETF)
CREATE OR REPLACE TABLE DM_DIM.DIM_SECURITY_ATTRIBUTES (
    SYMBOL          VARCHAR(50)  NOT NULL,
    SECURITY_NAME   VARCHAR(255),
    SECURITY_TYPE   VARCHAR(50),
    SECTOR          VARCHAR(100),  
    INDUSTRY        VARCHAR(150),
    WEBSITE_URL     VARCHAR(255)
);


-------------------------------- Views ---------------------------------------------------

-- View 1: 
create or replace view SA.VW_SECURITY_DAILY_PRICES as
select
  f.security_id,
  s.symbol,
  a.security_name,
  a.sector,
  a.industry,
  a.security_type,
  d.cal_date      as trade_date,
  f.open, 
  f.high, 
  f.low, 
  f.close, 
  f.volume
from DM_FACT.FACT_DAILY_PRICE f
join DM_DIM.DIM_DATE d
  on d.date_sk = f.date_sk
join DM_DIM.DIM_SECURITY s
  on s.security_id = f.security_id
left join DM_DIM.DIM_SECURITY_ATTRIBUTES a
  on a.symbol = s.symbol
where s.is_active = true;

comment on view SA.VW_SECURITY_DAILY_PRICES is
'Business-ready daily OHLCV joined with symbol and sector/industry (active universe only).';



-- View 2: 
create or replace view SA.VW_TOP20_EQUITY_BY_VOLUME_DAILY as
select
  p.trade_date,
  p.symbol,
  p.security_name,
  p.sector,
  p.industry,
  p.volume,
  p.close,
  (p.close * p.volume) as traded_value
from (
  select
    p.*,
    row_number() over (partition by p.trade_date order by p.volume desc) as rn
  from SA.VW_SECURITY_DAILY_PRICES p
  where p.security_type = 'Equity'
) p
where p.rn <= 20;

comment on view SA.VW_TOP20_EQUITY_BY_VOLUME_DAILY is
'Daily Top-20 most actively traded equities by volume with traded value (liquidity screen)';


-- view 3: Watch List (10 stocks)
create or replace view SA.VW_WATCHLIST_HISTORY as
with watchlist(symbol, rank_order, note) as (
  select column1, column2, column3 from values
    ('AAPL', 1, 'Mega-cap tech'),
    ('MSFT', 2, 'Cloud + AI'),
    ('NVDA', 3, 'Semis momentum'),
    ('AMZN', 4, 'Retail + cloud'),
    ('GOOGL',5, 'Search + AI'),
    ('META', 6, 'Ads + VR'),
    ('TSLA', 7, 'Auto + energy'),
    ('JPM',  8, 'Bank bellwether'),
    ('XOM',  9, 'Energy major'),
    ('UNH', 10, 'Healthcare payer')
)
select
  w.rank_order,
  p.trade_date,
  p.symbol,
  p.security_name,
  p.sector,
  p.industry,
  p.open, p.high, p.low, p.close, p.volume,
  (p.close * p.volume) as traded_value,
  w.note
from watchlist w
join SA.VW_SECURITY_DAILY_PRICES p
  on p.symbol = w.symbol
where p.security_type = 'Equity';


comment on view SA.VW_WATCHLIST_HISTORY is
'Time-series view of key watchlist equities including price, volume, and traded value — supports trend monitoring and focus dashboards.';



-- view 4: 

-- view 4: Last 30 calendar days with prior close + daily % return (streamlined)
create or replace view SA.VW_SECURITY_LAST_30D_DAILY_RETURN as
with bounds as (
  select max(p.trade_date) as max_dt,
         dateadd('day', -29, max(p.trade_date)) as min_dt
  from SA.VW_SECURITY_DAILY_PRICES p
),
base as (
  select
    p.trade_date,
    p.security_id,
    p.symbol,
    p.security_name,
    p.sector,
    p.industry,
    p.close,
    p.volume,
    lag(p.close) over (partition by p.security_id order by p.trade_date) as prev_close
  from SA.VW_SECURITY_DAILY_PRICES p
  where p.security_type = 'Equity'
)
select
  b.*,
  iff(b.prev_close is null, null, (b.close / nullif(b.prev_close, 0)) - 1) as daily_return
from base b
join bounds w
  on b.trade_date between w.min_dt and w.max_dt;


comment on view SA.VW_SECURITY_LAST_30D_DAILY_RETURN is
'Last 30 calendar days of equities with prior close (computed pre-filter) and daily % change.';



-- view 5
create or replace view SA.VW_SECTOR_LIQUIDITY_LATEST as
with last_day as (
  select max(trade_date) as dt
  from SA.VW_SECURITY_DAILY_PRICES
),
sector_liq as (
  select
    p.sector,
    d.dt as trade_date,
    count(*) as symbols_count,
    sum(p.close * p.volume) as traded_value
  from SA.VW_SECURITY_DAILY_PRICES p
  join last_day d
    on p.trade_date = d.dt
  where p.security_type = 'Equity'
  group by p.sector, d.dt
),
total_liq as (
  select trade_date, sum(traded_value) as total_traded_value
  from sector_liq
  group by trade_date
)
select
  s.trade_date,
  s.sector,
  s.symbols_count,
  s.traded_value,
  (s.traded_value / nullif(t.total_traded_value, 0)) as pct_contribution
from sector_liq s
join total_liq t
  on s.trade_date = t.trade_date
order by s.traded_value desc;



comment on view SA.VW_SECTOR_LIQUIDITY_LATEST is
'Latest trading-day sector liquidity snapshot: traded value, number of names, and % contribution of each sector to market liquidity.';



-- View 6

create or replace view SA.VW_ETF_LIQUIDITY_30D_SUMMARY as
with base as (
  select
    p.trade_date,
    p.security_id,
    p.symbol,
    p.security_name,
    p.close,
    p.volume,
    (p.close * p.volume) as traded_value
  from SA.VW_SECURITY_DAILY_PRICES p
  where p.security_type = 'ETF'
),
agg as (
  select
    security_id,
    symbol,
    security_name,
    avg(volume)       over (partition by security_id order by trade_date
                            rows between 29 preceding and current row) as avg_volume_30d,
    avg(traded_value) over (partition by security_id order by trade_date
                            rows between 29 preceding and current row) as avg_traded_value_30d,
    trade_date,
    close,
    volume,
    traded_value
  from base
),
latest as (
  select *
  from (
    select
      a.*,
      row_number() over (partition by security_id order by trade_date desc) as rn
    from agg a
  )
  where rn = 1
)
select
  trade_date as last_trade_date,
  security_id,
  symbol,
  security_name,
  avg_volume_30d,
  avg_traded_value_30d,
  close   as last_close,
  volume  as last_volume,
  traded_value as last_traded_value,
  row_number() over (order by avg_traded_value_30d desc nulls last) as liquidity_rank_30d
from latest
order by liquidity_rank_30d;

comment on view SA.VW_ETF_LIQUIDITY_30D_SUMMARY is
'ETF liquidity screener: 30-day average volume & traded value per ETF, with latest-day metrics and a 30D liquidity rank.';




------------- Helpers -------------------

-- select
--   table_schema,
--   table_name,
--   comment
-- from information_schema.views
-- where table_schema = 'SA';




-- select * from SEC_PRICING.SA.VW_SECURITY_DAILY_PRICES;
-- select * from SEC_PRICING.SA.VW_TOP20_EQUITY_BY_VOLUME_DAILY;
-- select * from SEC_PRICING.SA.VW_WATCHLIST_HISTORY;
-- select * from SEC_PRICING.SA.VW_SECURITY_LAST_30D_DAILY_RETURN;
-- select * from SA.VW_SECTOR_LIQUIDITY_LATEST;
-- select * from SA.VW_ETF_LIQUIDITY_30D_SUMMARY;


-- drop view SEC_PRICING.SA.VW_PRICE_BASE;