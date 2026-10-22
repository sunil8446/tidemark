select
    trade_date,
    venue,
    product_id,
    count(*) as trade_count,
    sum(size) as volume,
    sum(notional) as notional,
    cast(sum(notional) / nullif(sum(size), 0) as decimal(28, 12)) as vwap,
    min_by(price, trade_id) as open,
    max(price) as high,
    min(price) as low,
    max_by(price, trade_id) as close,
    cast(sum(case when taker_side = 'buy' then size else 0 end) / nullif(sum(size), 0) as double) as buy_volume_share,
    cast(count_if(source = 'rest') as double) / count(*) as rest_backfill_share,
    min(event_ts) as first_trade_ts,
    max(event_ts) as last_trade_ts
from {{ ref('stg_trades') }}
group by 1, 2, 3
