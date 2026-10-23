package dev.tidemark.flink.lake;

import dev.tidemark.flink.model.BookMsg;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * Materialises the full top-of-book every {@code intervalMs} of event time.
 *
 * <p>The feed only sends full snapshots on (re)subscribe, which can be hours
 * apart, so rebuilding a book "as of T" from deltas alone means replaying an
 * unbounded history. With a checkpoint every 10 s, a lookup is the nearest
 * checkpoint plus at most 10 s of deltas.
 *
 * <p>The book is truncated to {@code depth} after every message (Kraken never
 * deletes levels that drift out of the subscribed depth). Checkpoints are only
 * written once a real snapshot has been seen, so a partial book is never
 * presented as complete.
 *
 * <p>Checkpoints are cut in <b>book time</b>: before applying the first message at
 * or past a tick, the current state (every message before the tick, in partition
 * order) is written for that tick. An earlier version used event-time timers;
 * when the watermark lagged the book (catch-up, replay, an idle input), a timer
 * fired after later messages had been applied and wrote a future book under an
 * old timestamp, which crossed every book rebuilt from it. When a quiet book
 * skips several ticks, only the latest is written: the state is the same.
 */
public class BookCheckpoints extends KeyedProcessFunction<String, BookMsg, RowData> {
  private final long intervalMs;
  private final int depth;

  private transient MapState<String, BigDecimal> bids;
  private transient MapState<String, BigDecimal> asks;
  private transient ValueState<Boolean> haveSnapshot;
  private transient ValueState<Long> nextTick;

  public BookCheckpoints(long intervalMs, int depth) {
    this.intervalMs = intervalMs;
    this.depth = depth;
  }

  @Override
  public void open(Configuration parameters) {
    bids = getRuntimeContext().getMapState(new MapStateDescriptor<>("bids", Types.STRING, TypeInformation.of(BigDecimal.class)));
    asks = getRuntimeContext().getMapState(new MapStateDescriptor<>("asks", Types.STRING, TypeInformation.of(BigDecimal.class)));
    haveSnapshot = getRuntimeContext().getState(new ValueStateDescriptor<>("have-snapshot", Boolean.class));
    nextTick = getRuntimeContext().getState(new ValueStateDescriptor<>("next-tick", Long.class));
  }

  static String norm(String p) {
    return new BigDecimal(p).stripTrailingZeros().toPlainString();
  }

  @Override
  public void processElement(BookMsg m, Context ctx, Collector<RowData> out) throws Exception {
    long ts = m.eventTsUs / 1000;
    Long tick = nextTick.value();
    if (tick != null && ts >= tick) {
      long due = ts / intervalMs * intervalMs; // latest tick at or before this message
      checkpoint(ctx.getCurrentKey(), due, out);
    }
    if (m.isSnapshot) {
      bids.clear();
      asks.clear();
      haveSnapshot.update(true);
    }
    apply(m.bids, bids);
    apply(m.asks, asks);
    truncate(bids, true);
    truncate(asks, false);
    if (tick == null || ts >= tick) {
      nextTick.update((ts / intervalMs + 1) * intervalMs);
    }
  }

  private void checkpoint(String key, long ts, Collector<RowData> out) throws Exception {
    if (Boolean.TRUE.equals(haveSnapshot.value())) {
      int sep = key.indexOf(':');
      emit(out, key.substring(0, sep), key.substring(sep + 1), ts, "bid", bids, true);
      emit(out, key.substring(0, sep), key.substring(sep + 1), ts, "ask", asks, false);
    }
  }

  private static void apply(List<BookMsg.Level> levels, MapState<String, BigDecimal> side) throws Exception {
    for (BookMsg.Level l : levels) {
      BigDecimal q = new BigDecimal(l.qty);
      if (q.signum() == 0) {
        side.remove(norm(l.price));
      } else {
        side.put(norm(l.price), q);
      }
    }
  }

  private void truncate(MapState<String, BigDecimal> side, boolean bid) throws Exception {
    List<BigDecimal> prices = sorted(side, bid);
    for (BigDecimal p : prices.subList(Math.min(depth, prices.size()), prices.size())) {
      side.remove(p.stripTrailingZeros().toPlainString());
    }
  }

  private static List<BigDecimal> sorted(MapState<String, BigDecimal> side, boolean bid) throws Exception {
    List<BigDecimal> prices = new ArrayList<>();
    for (String p : side.keys()) {
      prices.add(new BigDecimal(p));
    }
    prices.sort(bid ? Comparator.reverseOrder() : Comparator.naturalOrder());
    return prices;
  }

  private void emit(Collector<RowData> out, String venue, String product, long ts, String sideName,
      MapState<String, BigDecimal> side, boolean bid) throws Exception {
    int level = 1;
    for (BigDecimal p : sorted(side, bid)) {
      out.collect(GenericRowData.of(Rows.str(venue), Rows.str(product), Rows.ts(ts * 1000), Rows.str(sideName),
          level++, Rows.dec(p), Rows.dec(side.get(p.stripTrailingZeros().toPlainString()))));
    }
  }

  /** For tests. */
  Map<String, BigDecimal> view(boolean bid) throws Exception {
    Map<String, BigDecimal> m = new java.util.LinkedHashMap<>();
    for (BigDecimal p : sorted(bid ? bids : asks, bid)) {
      m.put(p.toPlainString(), (bid ? bids : asks).get(p.stripTrailingZeros().toPlainString()));
    }
    return m;
  }
}
