-- Per day, venue and product: the trade-id range seen and how much of it is present.
with raw_counts as (
    select
        venue,
        product_id,
        cast(event_ts as date) as trade_date,
        count(*) as raw_rows
    from {{ source('market', 'raw_trades') }}
    group by 1, 2, 3
),

ids as (
    select
        venue,
        product_id,
        trade_date,
        min(trade_id) as min_trade_id,
        max(trade_id) as max_trade_id,
        count(*) as present_ids,
        count_if(source = 'rest') as rest_ids
    from {{ ref('stg_trades') }}
    group by 1, 2, 3
)

select
    i.venue,
    i.product_id,
    i.trade_date,
    i.min_trade_id,
    i.max_trade_id,
    i.max_trade_id - i.min_trade_id + 1 as expected_ids,
    i.present_ids,
    i.rest_ids,
    r.raw_rows - i.present_ids as duplicate_rows
from ids as i
join raw_counts as r
    on i.venue = r.venue
    and i.product_id = r.product_id
    and i.trade_date = r.trade_date
