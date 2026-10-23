package dev.tidemark.flink;

import dev.tidemark.flink.analytics.Bar;
import dev.tidemark.flink.analytics.BarAccumulator;
import dev.tidemark.flink.analytics.BarWindow;
import dev.tidemark.flink.analytics.Spread;
import dev.tidemark.flink.analytics.Vwap;
import dev.tidemark.flink.clickhouse.ClickHouseSink;
import dev.tidemark.flink.clickhouse.Json;
import dev.tidemark.flink.dedup.DedupTrades;
import dev.tidemark.flink.kafka.Deserializers;
import dev.tidemark.flink.model.TradeEvent;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

/**
 * Hot aggregates for ClickHouse: 1 s and 1 m OHLCV bars, a rolling 5 m VWAP,
 * and the Coinbase–Kraken spread per product sampled every second.
 *
 * <p>Late trades (typically REST backfill after a gap) are accepted for
 * {@code --allowed-lateness-s}; each late trade re-fires its window and the
 * corrected row replaces the old one in ClickHouse (ReplacingMergeTree).
 * Trades later than that go to a side output and are counted.
 */
public class AnalyticsJob {

  public static void main(String[] args) throws Exception {
    ParameterTool p = ParameterTool.fromArgs(args);
    String brokers = p.get("brokers", "redpanda-0:9092");
    String group = p.get("group", "tidemark-analytics");
    String suffix = p.get("topic-suffix", "");
    Duration outOfOrder = Duration.ofSeconds(p.getLong("out-of-order-s", 5));
    Duration idle = Duration.ofSeconds(p.getLong("idle-s", 30));
    Duration lateness = Duration.ofSeconds(p.getLong("allowed-lateness-s", 300));
    String chUrl = p.get("clickhouse.url", "http://clickhouse:8123");
    String chUser = p.get("clickhouse.user", "tidemark");
    String chPassword = p.get("clickhouse.password", "tidemark-local-dev");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(p);
    env.enableCheckpointing(p.getLong("checkpoint-ms", 30_000), CheckpointingMode.EXACTLY_ONCE);
    CheckpointConfig cc = env.getCheckpointConfig();
    // Keep checkpoints when the job is cancelled, so it can always be restored (it can be
    // impossible to take a savepoint from a job that is struggling), and checkpoint
    // unaligned so barriers do not queue behind a catch-up backlog.
    cc.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    cc.enableUnalignedCheckpoints();
    cc.setTolerableCheckpointFailureNumber(5);

    WatermarkStrategy<TradeEvent> wm = WatermarkStrategy.<TradeEvent>forBoundedOutOfOrderness(outOfOrder)
        .withTimestampAssigner((t, ts) -> t.eventTsUs / 1000)
        .withIdleness(idle);
    DataStream<TradeEvent> trades = env
        .fromSource(LakehouseJob.source(brokers, "trades.v1" + suffix, group, new Deserializers.Trades()), wm, "trades.v1")
        .uid("src-trades").name("trades.v1")
        .keyBy(TradeEvent::tradeKey)
        .process(new DedupTrades(Duration.ofHours(p.getLong("dedup-ttl-h", 24))))
        .uid("dedup-trades").name("dedup (venue, product, trade_id)");

    OutputTag<TradeEvent> tooLate = new OutputTag<>("too-late") {};

    SingleOutputStreamOperator<Bar> bars1s = trades
        .keyBy(TradeEvent::instrumentKey)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(1)))
        .allowedLateness(lateness)
        .sideOutputLateData(tooLate)
        .aggregate(new BarAccumulator.Agg(), new BarWindow("1s"))
        .uid("bars-1s").name("bars 1s");
    SingleOutputStreamOperator<Bar> bars1m = trades
        .keyBy(TradeEvent::instrumentKey)
        .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
        .allowedLateness(lateness)
        .sideOutputLateData(tooLate)
        .aggregate(new BarAccumulator.Agg(), new BarWindow("1m"))
        .uid("bars-1m").name("bars 1m");
    DataStream<Vwap> vwap = trades
        .keyBy(TradeEvent::instrumentKey)
        .window(SlidingEventTimeWindows.of(Duration.ofMinutes(5), Duration.ofSeconds(10)))
        .aggregate(new BarAccumulator.Agg(), new Vwap.Fn("5m"))
        .uid("vwap-5m").name("rolling VWAP 5m / 10s");
    DataStream<Spread> spreads = trades
        .keyBy(t -> t.productId)
        .process(new Spread.Fn(p.getLong("spread-max-stale-ms", 60_000)))
        .uid("spread").name("cross-venue spread 1s");

    int maxRows = p.getInt("clickhouse.batch", 5_000);
    long flushMs = p.getLong("clickhouse.flush-ms", 1_000);
    bars1s.union(bars1m)
        .sinkTo(new ClickHouseSink<>(chUrl, chUser, chPassword, "tidemark.bars", Json::bar, maxRows, flushMs))
        .uid("ch-bars").name("clickhouse: bars");
    vwap.sinkTo(new ClickHouseSink<>(chUrl, chUser, chPassword, "tidemark.vwap", Json::vwap, maxRows, flushMs))
        .uid("ch-vwap").name("clickhouse: vwap");
    spreads.sinkTo(new ClickHouseSink<>(chUrl, chUser, chPassword, "tidemark.spreads", Json::spread, maxRows, flushMs))
        .uid("ch-spreads").name("clickhouse: spreads");

    // Count trades too late even for allowed lateness; the lakehouse still has them.
    bars1s.getSideOutput(tooLate).union(bars1m.getSideOutput(tooLate))
        .map(t -> 1).uid("too-late-count").name("too-late trades")
        .addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>()).name("discard");

    env.execute("tidemark-analytics");
  }
}
