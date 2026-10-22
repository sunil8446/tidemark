-- Gaps-and-islands: every run of trade ids absent from the lakehouse.
-- Input for the reconciliation job, which re-fetches these ranges over REST.
with ordered as (
    select
        venue,
        product_id,
        trade_id,
        event_ts,
        lead(trade_id) over (partition by venue, product_id order by trade_id) as next_trade_id,
        lead(event_ts) over (partition by venue, product_id order by trade_id) as next_event_ts
    from {{ ref('stg_trades') }}
)

select
    venue,
    product_id,
    trade_id + 1 as first_missing_trade_id,
    next_trade_id - 1 as last_missing_trade_id,
    next_trade_id - trade_id - 1 as missing_ids,
    event_ts as before_event_ts,
    next_event_ts as after_event_ts
from ordered
where next_trade_id > trade_id + 1
