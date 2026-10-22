-- Hot aggregates written by the Flink analytics job.
-- ReplacingMergeTree keyed on the natural key: a replayed or late-corrected
-- row replaces the earlier one at merge time. Query with FINAL (or argMax on
-- the version column) to read the latest version before merges complete.

CREATE DATABASE IF NOT EXISTS tidemark;

CREATE TABLE IF NOT EXISTS tidemark.bars
(
    venue        LowCardinality(String),
    product_id   LowCardinality(String),
    interval     LowCardinality(String),
    bar_start    DateTime64(3, 'UTC'),
    open         Decimal(38, 12),
    high         Decimal(38, 12),
    low          Decimal(38, 12),
    close        Decimal(38, 12),
    volume       Decimal(38, 12),
    notional     Decimal(38, 12),
    vwap         Nullable(Decimal(38, 12)),
    buy_volume   Decimal(38, 12),
    sell_volume  Decimal(38, 12),
    trade_count  UInt32,
    revision     UInt32,
    inserted_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(bar_start)
ORDER BY (venue, product_id, interval, bar_start)
TTL toDateTime(bar_start) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS tidemark.vwap
(
    venue        LowCardinality(String),
    product_id   LowCardinality(String),
    window       LowCardinality(String),
    window_end   DateTime64(3, 'UTC'),
    vwap         Nullable(Decimal(38, 12)),
    volume       Decimal(38, 12),
    trade_count  UInt32,
    inserted_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(window_end)
ORDER BY (venue, product_id, window, window_end)
TTL toDateTime(window_end) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS tidemark.spreads
(
    product_id       LowCardinality(String),
    ts               DateTime64(3, 'UTC'),
    coinbase_price   Decimal(38, 12),
    kraken_price     Decimal(38, 12),
    coinbase_age_ms  UInt32,
    kraken_age_ms    UInt32,
    spread           Decimal(38, 12),
    spread_bps       Float64,
    inserted_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(ts)
ORDER BY (product_id, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY;

-- Detector firings. stream = 'live' for the production detector job,
-- 'inj' for the evaluation job reading injected (labelled) replays.
CREATE TABLE IF NOT EXISTS tidemark.alerts
(
    stream        LowCardinality(String),
    alert_id      String,
    detector      LowCardinality(String),
    venue         LowCardinality(String),
    product_id    LowCardinality(String),
    event_ts      DateTime64(3, 'UTC'),
    window_start  DateTime64(3, 'UTC'),
    window_end    DateTime64(3, 'UTC'),
    score         Float64,
    threshold     Float64,
    severity      LowCardinality(String),
    summary       String,
    evidence      Map(String, String),
    emitted_at    DateTime64(3, 'UTC'),
    inserted_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (stream, alert_id);
