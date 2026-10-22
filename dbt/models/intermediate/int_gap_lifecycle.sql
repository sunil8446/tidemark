-- Collapse DETECTED + terminal events into one row per gap.
with events as (
    select * from {{ ref('stg_gap_events') }}
),

detected as (
    select
        gap_id,
        min(detected_ts) as detected_ts
    from events
    group by 1
),

terminal as (
    select
        gap_id,
        max_by(status, resolved_ts) as status,
        max(resolved_ts) as resolved_ts,
        max_by(recovered_count, resolved_ts) as recovered_count,
        max_by(detail, resolved_ts) as detail
    from events
    where status <> 'detected'
    group by 1
),

attrs as (
    select
        gap_id,
        arbitrary(venue) as venue,
        arbitrary(product_id) as product_id,
        arbitrary(kind) as kind,
        arbitrary(first_missing_trade_id) as first_missing_trade_id,
        arbitrary(last_missing_trade_id) as last_missing_trade_id,
        arbitrary(span) as span
    from events
    group by 1
)

select
    a.gap_id,
    a.venue,
    a.product_id,
    a.kind,
    coalesce(t.status, 'open') as status,
    a.first_missing_trade_id,
    a.last_missing_trade_id,
    a.span,
    t.recovered_count,
    d.detected_ts,
    t.resolved_ts,
    t.detail
from attrs as a
join detected as d on a.gap_id = d.gap_id
left join terminal as t on a.gap_id = t.gap_id
