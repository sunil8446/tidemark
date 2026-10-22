package dev.tidemark.flink;

import dev.tidemark.flink.dedup.DedupTrades;
import dev.tidemark.flink.kafka.Deserializers;
import dev.tidemark.flink.lake.Lakehouse;
import dev.tidemark.flink.lake.BookCheckpoints;
import dev.tidemark.flink.lake.Rows;
import dev.tidemark.flink.model.BookLevelEvent;
import dev.tidemark.flink.model.BookMsg;
import dev.tidemark.flink.model.GapRecord;
import dev.tidemark.flink.model.TradeEvent;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * trades.v1, book.v1, gaps.v1 → Iceberg (market.raw_trades, market.book_deltas, market.gap_events).
 *
 * <p>Exactly-once end to end: Kafka offsets and dedup state are snapshotted in
 * each checkpoint, and the Iceberg sink commits the files written since the
 * previous checkpoint only when that checkpoint completes. On failure the job
 * rewinds to the last completed checkpoint; uncommitted files are never
 * visible, so nothing is lost or written twice.
 */
public class LakehouseJob {

  public static void main(String[] args) throws Exception {
    ParameterTool p = ParameterTool.fromArgs(args);
    String brokers = p.get("brokers", "redpanda-0:9092");
    String group = p.get("group", "tidemark-lakehouse");
    String suffix = p.get("topic-suffix", "");
    long checkpointMs = p.getLong("checkpoint-ms", 60_000);
    Duration outOfOrder = Duration.ofSeconds(p.getLong("out-of-order-s", 5));
    Duration idle = Duration.ofSeconds(p.getLong("idle-s", 30));
    Duration dedupTtl = Duration.ofHours(p.getLong("dedup-ttl-h", 24));
    // market for live data; market_inj when lakehousing the injected evaluation stream.
    String namespace = p.get("namespace", Lakehouse.NAMESPACE);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(p);
    env.enableCheckpointing(checkpointMs, CheckpointingMode.EXACTLY_ONCE);
    CheckpointConfig cc = env.getCheckpointConfig();
    cc.setMinPauseBetweenCheckpoints(checkpointMs / 2);
    cc.setCheckpointTimeout(Duration.ofMinutes(5).toMillis());
    cc.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    cc.setTolerableCheckpointFailureNumber(3);

    CatalogLoader catalog = Lakehouse.catalogLoader(p);
    TableLoader tradesTable = Lakehouse.ensureTable(catalog, namespace, "raw_trades", Lakehouse.RAW_TRADES, "event_ts");
    TableLoader bookTable = Lakehouse.ensureTable(catalog, namespace, "book_deltas", Lakehouse.BOOK_DELTAS, "event_ts");
    TableLoader gapsTable = Lakehouse.ensureTable(catalog, namespace, "gap_events", Lakehouse.GAP_EVENTS, "detected_ts");
    TableLoader snapshotsTable = Lakehouse.ensureTable(catalog, namespace, "book_snapshots", Lakehouse.BOOK_SNAPSHOTS, "snapshot_ts");

    // Trades: event-time watermarks, then dedup on (venue, product, trade_id).
    WatermarkStrategy<TradeEvent> tradeWm = WatermarkStrategy.<TradeEvent>forBoundedOutOfOrderness(outOfOrder)
        .withTimestampAssigner((t, ts) -> t.eventTsUs / 1000)
        .withIdleness(idle);
    DataStream<TradeEvent> trades = env
        .fromSource(source(brokers, "trades.v1" + suffix, group, new Deserializers.Trades()), tradeWm, "trades.v1")
        .uid("src-trades").name("trades.v1");
    DataStream<TradeEvent> unique = trades
        .keyBy(TradeEvent::tradeKey)
        .process(new DedupTrades(dedupTtl))
        .uid("dedup-trades").name("dedup (venue, product, trade_id)");

    DataStream<RowData> tradeRows = unique.map(Rows::trade)
        .returns(InternalTypeInfo.of(FlinkSchemaUtil.convert(Lakehouse.RAW_TRADES)))
        .uid("rows-trades").name("to RowData: raw_trades");
    FlinkSink.forRowData(tradeRows).tableLoader(tradesTable).writeParallelism(1)
        .uidPrefix("sink-raw-trades").append();

    // Book levels: no dedup (seq is per ingestor session); append as delivered.
    WatermarkStrategy<BookLevelEvent> bookWm = WatermarkStrategy.<BookLevelEvent>forBoundedOutOfOrderness(outOfOrder)
        .withTimestampAssigner((b, ts) -> b.eventTsUs / 1000)
        .withIdleness(idle);
    DataStream<RowData> bookRows = env
        .fromSource(source(brokers, "book.v1" + suffix, group, new Deserializers.Books()), bookWm, "book.v1")
        .uid("src-book").name("book.v1")
        .map(Rows::book)
        .returns(InternalTypeInfo.of(FlinkSchemaUtil.convert(Lakehouse.BOOK_DELTAS)))
        .uid("rows-book").name("to RowData: book_deltas");
    FlinkSink.forRowData(bookRows).tableLoader(bookTable).writeParallelism(1)
        .uidPrefix("sink-book-deltas").append();

    // Top-of-book checkpoints every 10 s, so "the book at time T" is a bounded lookup.
    WatermarkStrategy<BookMsg> bookMsgWm = WatermarkStrategy.<BookMsg>forBoundedOutOfOrderness(outOfOrder)
        .withTimestampAssigner((b, ts) -> b.eventTsUs / 1000)
        .withIdleness(idle);
    DataStream<RowData> snapshotRows = env
        .fromSource(source(brokers, "book.v1" + suffix, group + "-snapshots", new Deserializers.BookMessages()), bookMsgWm,
            "book.v1 (messages)")
        .uid("src-book-msgs").name("book.v1 (messages)")
        .keyBy(BookMsg::instrumentKey)
        .process(new BookCheckpoints(p.getLong("book-checkpoint-ms", 10_000), p.getInt("kraken-book-depth", 10)))
        .returns(InternalTypeInfo.of(FlinkSchemaUtil.convert(Lakehouse.BOOK_SNAPSHOTS)))
        .uid("book-checkpoints").name("book checkpoints 10s");
    FlinkSink.forRowData(snapshotRows).tableLoader(snapshotsTable).writeParallelism(1)
        .uidPrefix("sink-book-snapshots").append();

    // Gap lifecycle events, for the gap ledger mart.
    DataStream<RowData> gapRows = env
        .fromSource(source(brokers, "gaps.v1" + suffix, group, new Deserializers.Gaps()),
            WatermarkStrategy.<GapRecord>noWatermarks(), "gaps.v1")
        .uid("src-gaps").name("gaps.v1")
        .map(Rows::gap)
        .returns(InternalTypeInfo.of(FlinkSchemaUtil.convert(Lakehouse.GAP_EVENTS)))
        .uid("rows-gaps").name("to RowData: gap_events");
    FlinkSink.forRowData(gapRows).tableLoader(gapsTable).writeParallelism(1)
        .uidPrefix("sink-gap-events").append();

    env.execute(Lakehouse.NAMESPACE.equals(namespace) ? "tidemark-lakehouse" : "tidemark-lakehouse-" + namespace);
  }

  static <T> KafkaSource<T> source(String brokers, String topic, String group, KafkaRecordDeserializationSchema<T> d) {
    return KafkaSource.<T>builder()
        .setBootstrapServers(brokers)
        .setTopics(topic)
        .setGroupId(group)
        // Committed offsets are only for lag monitoring; on restore the checkpointed offsets win.
        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
        .setDeserializer(d)
        .setProperty("commit.offsets.on.checkpoint", "true")
        .setProperty("partition.discovery.interval.ms", "60000")
        .setProperty("client.id.prefix", group + "-" + topic)
        .build();
  }
}
