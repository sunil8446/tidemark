-- One row per (venue, product_id, trade_id). Flink already dedups; the
-- row_number here makes the guarantee explicit and survives any replay
-- that bypassed Flink state (e.g. after the dedup TTL).
with ranked as (
    select
        venue,
        product_id,
        trade_id,
        price,
        size,
        price * size as notional,
        taker_side,
        event_ts,
        recv_ts,
        source,
        coalesce(from_snapshot, false) as from_snapshot,
        row_number() over (
            partition by venue, product_id, trade_id
            order by case when source = 'ws' then 0 else 1 end, recv_ts
        ) as copy_rank
    from {{ source('market', 'raw_trades') }}
)

select
    venue,
    product_id,
    trade_id,
    price,
    size,
    notional,
    taker_side,
    event_ts,
    recv_ts,
    source,
    from_snapshot,
    cast(event_ts as date) as trade_date
from ranked
where copy_rank = 1
