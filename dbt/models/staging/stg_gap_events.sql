select
    gap_id,
    venue,
    product_id,
    kind,
    status,
    first_missing_trade_id,
    last_missing_trade_id,
    case
        when kind = 'checksum_mismatch' then null
        else last_missing_trade_id - first_missing_trade_id + 1
    end as span,
    recovered_count,
    detected_ts,
    resolved_ts,
    detail
from {{ source('market', 'gap_events') }}
