-- One row per day, venue and product: is the lakehouse complete, and what
-- did the pipeline have to repair to get there?
with coverage as (
    select * from {{ ref('int_trade_id_coverage_daily') }}
),

gaps as (
    select
        cast(detected_ts as date) as trade_date,
        venue,
        product_id,
        count_if(kind <> 'checksum_mismatch') as gaps_detected,
        count_if(kind <> 'checksum_mismatch' and status in ('recovered', 'absent_at_source')) as gaps_recovered,
        count_if(kind <> 'checksum_mismatch' and status in ('unrecoverable', 'open')) as gaps_unrecovered,
        count_if(kind = 'checksum_mismatch') as checksum_resyncs
    from {{ ref('fct_gap_ledger') }}
    group by 1, 2, 3
),

books as (
    select
        cast(event_ts as date) as trade_date,
        venue,
        product_id,
        count(*) as book_messages,
        count_if(not checksum_ok and checksum > 0) as checksum_failures
    from {{ ref('stg_book_messages') }}
    group by 1, 2, 3
)

select
    c.trade_date,
    c.venue,
    c.product_id,
    c.expected_ids,
    c.present_ids,
    c.expected_ids - c.present_ids as missing_ids,
    c.duplicate_rows,
    c.rest_ids,
    cast(c.present_ids as double) / c.expected_ids as completeness,
    coalesce(g.gaps_detected, 0) as gaps_detected,
    coalesce(g.gaps_recovered, 0) as gaps_recovered,
    coalesce(g.gaps_unrecovered, 0) as gaps_unrecovered,
    coalesce(g.checksum_resyncs, 0) as checksum_resyncs,
    b.book_messages,
    b.checksum_failures
from coverage as c
left join gaps as g
    on c.trade_date = g.trade_date and c.venue = g.venue and c.product_id = g.product_id
left join books as b
    on c.trade_date = b.trade_date and c.venue = b.venue and c.product_id = b.product_id
