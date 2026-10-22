-- 1-minute bars recomputed from the lakehouse (the source of truth),
-- independent of the streaming bars in ClickHouse. Open/close by trade_id,
-- which increases with time per product.
select
    venue,
    product_id,
    date_trunc('minute', event_ts) as bar_start,
    min_by(price, trade_id) as open,
    max(price) as high,
    min(price) as low,
    max_by(price, trade_id) as close,
    sum(size) as volume,
    sum(notional) as notional,
    cast(sum(notional) / nullif(sum(size), 0) as decimal(28, 12)) as vwap,
    count(*) as trade_count
from {{ ref('stg_trades') }}
group by 1, 2, 3
