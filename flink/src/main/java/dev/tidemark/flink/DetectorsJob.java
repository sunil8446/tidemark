package dev.tidemark.flink;

import dev.tidemark.flink.analytics.Spread;
import dev.tidemark.flink.clickhouse.ClickHouseSink;
import dev.tidemark.flink.clickhouse.Json;
import dev.tidemark.flink.dedup.DedupTrades;
import dev.tidemark.flink.detect.AlertEvent;
import dev.tidemark.flink.detect.AlertSerializer;
import dev.tidemark.flink.detect.Dislocation;
import dev.tidemark.flink.detect.SpoofLayering;
import dev.tidemark.flink.detect.StampAlerts;
import dev.tidemark.flink.detect.VolumeSpike;
import dev.tidemark.flink.detect.WashTrade;
import dev.tidemark.flink.kafka.Deserializers;
import dev.tidemark.flink.kafka.SchemaRegistry;
import dev.tidemark.flink.model.BookMsg;
import dev.tidemark.flink.model.TradeEvent;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Real-time detectors: volume spike, spoof-like layering, cross-venue
 * dislocation, wash trading. Alerts go to alerts.v1 (Protobuf, for the
 * investigator) and ClickHouse tidemark.alerts (for dashboards and lookups).
 *
 * <p>Run once on live topics (stream=live) with tuned thresholds, and once on
 * the injected replay topics (--topic-suffix .inj --stream inj) with low
 * thresholds, so every candidate is scored and precision/recall can be swept
 * offline.
 */
public class DetectorsJob {

  public static void main(String[] args) throws Exception {
    ParameterTool p = ParameterTool.fromArgs(args);
    String brokers = p.get("brokers", "redpanda-0:9092");
    String registry = p.get("registry", "http://redpanda-0:8081");
    String suffix = p.get("topic-suffix", "");
    String stream = p.get("stream", "live");
    String group = p.get("group", "tidemark-detectors-" + stream);
    Duration outOfOrder = Duration.ofSeconds(p.getLong("out-of-order-s", 5));
    Duration idle = Duration.ofSeconds(p.getLong("idle-s", 30));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    if (p.has("state-backend")) {
      // hashmap for evaluation runs: detector state is small and heap state is several times faster than RocksDB.
      Configuration conf = new Configuration();
      conf.setString("state.backend.type", p.get("state-backend"));
      env.configure(conf);
    }
    env.getConfig().setGlobalJobParameters(p);
    env.enableCheckpointing(p.getLong("checkpoint-ms", 30_000), CheckpointingMode.EXACTLY_ONCE);
    CheckpointConfig cc = env.getCheckpointConfig();
    // Keep checkpoints when the job is cancelled, so it can always be restored (it can be
    // impossible to take a savepoint from a job that is struggling), and checkpoint
    // unaligned so barriers do not queue behind a catch-up backlog.
    cc.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    cc.enableUnalignedCheckpoints();
    cc.setTolerableCheckpointFailureNumber(5);

    WatermarkStrategy<TradeEvent> tradeWm = WatermarkStrategy.<TradeEvent>forBoundedOutOfOrderness(outOfOrder)
        .withTimestampAssigner((t, ts) -> t.eventTsUs / 1000).withIdleness(idle);
    DataStream<TradeEvent> trades = env
        .fromSource(LakehouseJob.source(brokers, "trades.v1" + suffix, group, new Deserializers.Trades()), tradeWm, "trades")
        .uid("src-trades").name("trades.v1" + suffix)
        .keyBy(TradeEvent::tradeKey)
        .process(new DedupTrades(Duration.ofHours(p.getLong("dedup-ttl-h", 24))))
        .uid("dedup-trades").name("dedup");

    WatermarkStrategy<BookMsg> bookWm = WatermarkStrategy.<BookMsg>forBoundedOutOfOrderness(outOfOrder)
        .withTimestampAssigner((b, ts) -> b.eventTsUs / 1000).withIdleness(idle);
    DataStream<BookMsg> books = env
        .fromSource(LakehouseJob.source(brokers, "book.v1" + suffix, group, new Deserializers.BookMessages()), bookWm, "book")
        .uid("src-book").name("book.v1" + suffix);

    DataStream<AlertEvent> spikes = trades
        .keyBy(TradeEvent::instrumentKey)
        .process(new VolumeSpike(
            p.getInt("spike.window-s", 10),
            p.getInt("spike.baseline-s", 1800),
            p.getInt("spike.min-baseline", 300),
            p.getDouble("spike.z", 6.0),
            p.getDouble("spike.min-multiple", 3.0),
            p.getDouble("spike.min-notional", 25_000),
            p.getDouble("spike.impact-bps", 5.0)))
        .uid("det-volume-spike").name("volume spike");

    DataStream<AlertEvent> spoofs = books
        .keyBy(BookMsg::instrumentKey)
        .connect(trades.keyBy(TradeEvent::instrumentKey))
        .process(new SpoofLayering(
            p.getInt("spoof.near-levels", 3),
            p.getDouble("spoof.depth-share", 1.0),
            p.getLong("spoof.max-life-ms", 10_000),
            p.getDouble("spoof.pulled-fraction", 0.8),
            p.getDouble("spoof.max-traded-fraction", 0.1),
            p.getLong("spoof.min-life-ms", 250),
            p.getLong("spoof.reprice-ms", 1_000),
            p.getInt("kraken-book-depth", 10)))
        .uid("det-spoof").name("spoof-like layering");

    DataStream<AlertEvent> dislocations = trades
        .keyBy(t -> t.productId)
        .process(new Spread.Fn(p.getLong("spread-max-stale-ms", 30_000)))
        .uid("spread").name("spread 1s")
        .keyBy(s -> s.productId)
        .process(new Dislocation(
            p.getInt("dislocation.baseline-samples", 1800),
            p.getInt("dislocation.min-baseline", 300),
            p.getDouble("dislocation.z", 5.0),
            p.getDouble("dislocation.min-bps", 3.0),
            p.getInt("dislocation.sustain-s", 5),
            p.getLong("dislocation.max-age-ms", 5_000)))
        .uid("det-dislocation").name("dislocation");

    DataStream<AlertEvent> washes = trades
        .keyBy(TradeEvent::instrumentKey)
        .process(new WashTrade(p.getLong("wash.window-ms", 60_000), p.getDouble("wash.min-score", 0.95)))
        .uid("det-wash").name("wash trade");

    DataStream<AlertEvent> alerts = spikes.union(spoofs, dislocations, washes)
        .map(new StampAlerts("live".equals(stream), p.getLong("alert-slo-ms", 30_000)))
        .uid("stamp").name("stamp emitted_at");

    String topic = "alerts.v1" + suffix;
    int schemaId = SchemaRegistry.register(registry, topic + "-value", SchemaRegistry.marketProto());
    alerts.sinkTo(KafkaSink.<AlertEvent>builder()
            .setBootstrapServers(brokers)
            .setRecordSerializer(new AlertSerializer(topic, schemaId))
            // Alert ids are deterministic; consumers upsert, so at-least-once is enough.
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build())
        .uid("sink-alerts-kafka").name(topic);
    alerts.sinkTo(new ClickHouseSink<>(p.get("clickhouse.url", "http://clickhouse:8123"),
            p.get("clickhouse.user", "tidemark"), p.get("clickhouse.password", "tidemark-local-dev"),
            "tidemark.alerts", a -> Json.alert(a, stream), 1_000, 1_000))
        .uid("sink-alerts-ch").name("clickhouse: alerts");

    env.execute("tidemark-detectors-" + stream);
  }
}
