package dev.tidemark.flink.dedup;

import dev.tidemark.flink.model.TradeEvent;
import java.time.Duration;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Keyed by (venue, product, trade_id): emits the first copy of each trade and
 * drops the rest. The "seen" flag expires after {@code ttl}, which bounds state
 * while still covering restarts, REST backfill overlap and replays.
 *
 * <p>The flag lives in Flink state, so it is part of every checkpoint: after a
 * failure the job restores the flags and the Kafka offsets together, and a
 * trade emitted before the checkpoint is not emitted again.
 */
public class DedupTrades extends KeyedProcessFunction<String, TradeEvent, TradeEvent> {
  private final Duration ttl;
  private transient ValueState<Boolean> seen;
  private transient Counter duplicates;
  private transient Counter unique;

  public DedupTrades(Duration ttl) {
    this.ttl = ttl;
  }

  @Override
  public void open(Configuration parameters) {
    StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(ttl)
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .cleanupFullSnapshot()
        .build();
    ValueStateDescriptor<Boolean> d = new ValueStateDescriptor<>("seen", Types.BOOLEAN);
    d.enableTimeToLive(ttlConfig);
    seen = getRuntimeContext().getState(d);
    duplicates = getRuntimeContext().getMetricGroup().counter("tidemark_duplicates_dropped");
    unique = getRuntimeContext().getMetricGroup().counter("tidemark_unique_trades");
  }

  @Override
  public void processElement(TradeEvent t, Context ctx, Collector<TradeEvent> out) throws Exception {
    if (seen.value() != null) {
      duplicates.inc();
      return;
    }
    seen.update(Boolean.TRUE);
    unique.inc();
    out.collect(t);
  }
}
