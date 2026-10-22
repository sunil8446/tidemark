{#
  Trade ids must increase with event time per venue and product. A row fails
  when a higher trade_id carries an event_ts earlier than the previous id's by
  more than `tolerance_seconds` (venues stamp to the microsecond, but a little
  jitter between matching-engine and timestamping is normal).
#}
{% test monotonic_trade_id(model, tolerance_seconds=1) %}
with ordered as (
    select
        venue,
        product_id,
        trade_id,
        event_ts,
        lag(event_ts) over (partition by venue, product_id order by trade_id) as prev_event_ts
    from {{ model }}
)
select *
from ordered
where prev_event_ts is not null
  and event_ts < prev_event_ts - interval '{{ tolerance_seconds }}' second
{% endtest %}
