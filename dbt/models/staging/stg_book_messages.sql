-- book_deltas has one row per level; collapse back to one row per message.
select
    venue,
    product_id,
    kafka_partition,
    kafka_offset,
    max(seq) as seq,
    bool_or(is_snapshot) as is_snapshot,
    bool_and(checksum_ok) as checksum_ok,
    max(checksum) as checksum,
    min(event_ts) as event_ts,
    count(*) as level_count
from {{ source('market', 'book_deltas') }}
group by 1, 2, 3, 4
