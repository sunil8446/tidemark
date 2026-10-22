select
    gap_id,
    venue,
    product_id,
    kind,
    status,
    first_missing_trade_id,
    last_missing_trade_id,
    span,
    recovered_count,
    detected_ts,
    resolved_ts,
    cast(date_diff('millisecond', detected_ts, resolved_ts) as double) / 1000 as recovery_seconds,
    detail
from {{ ref('int_gap_lifecycle') }}
