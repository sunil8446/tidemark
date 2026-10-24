package dev.tidemark.flink.analytics;

import dev.tidemark.flink.model.TradeEvent;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Cross-venue spread for one product: the last traded price on each venue,
 * sampled once per event-time second. A venue whose last trade is older than
 * {@code maxStaleMs} is treated as unknown, so a quiet venue does not produce
 * a fake dislocation.
 *
 * <p>The sample at tick T uses, per venue, the latest trade at or before T
 * (an as-of lookup over a short per-venue history), so trades that arrive
 * out of order never leak a later price into an earlier sample.
 *
 * <p>A tick is final once <b>both</b> venues' trades have moved past it by
 * {@code latenessMs}, or once the watermark passes it (the backstop for a quiet
 * venue). The two venues arrive through different Kafka partitions, which can
 * be minutes apart during catch-up or replay, so finality must wait for the
 * slower one. (A version that finalised on the faster venue treated the other
 * venue's in-flight trades as stale and missed 6 of 19 injected dislocations.)
 *
 * <p>History is kept per venue in one map entry per second, so adding a trade
 * and the as-of lookup are constant time however far one venue runs ahead.
 * (Keeping it as one list value made every trade re-serialise the whole list:
 * quadratic once the watermark lagged, and the job stopped checkpointing.)
 */
public class Spread {
  public String productId;
  public long tsMs;
  public BigDecimal coinbasePrice;
  public BigDecimal krakenPrice;
  public long coinbaseAgeMs;
  public long krakenAgeMs;
  public BigDecimal spread; // coinbase - kraken
  public double spreadBps; // relative to the mid

  public Spread() {}

  /** Last trade seen on one venue. */
  public static class Last {
    public BigDecimal price;
    public long tsMs;
    public long tradeId;

    public Last() {}
  }

  public static class Fn extends KeyedProcessFunction<String, TradeEvent, Spread> {
    private final long maxStaleMs;
    private final long latenessMs;
    private transient Venue coinbase;
    private transient Venue kraken;
    private transient ValueState<Long> nextTick;
    private transient ValueState<Long> lastSampled;

    public Fn(long maxStaleMs) {
      this(maxStaleMs, 5_000);
    }

    /** @param latenessMs how far both venues must be past a tick before it is final (the source's out-of-orderness). */
    public Fn(long maxStaleMs, long latenessMs) {
      this.maxStaleMs = maxStaleMs;
      this.latenessMs = latenessMs;
    }

    /** One venue's trades for this product, bucketed by second. */
    static final class Venue {
      final MapState<Long, ArrayList<Last>> bySecond;
      final ValueState<Long> newest; // latest trade time seen (ms)
      final ValueState<Long> oldest; // oldest second still stored

      @SuppressWarnings({"unchecked", "rawtypes"})
      Venue(org.apache.flink.api.common.functions.RuntimeContext rc, String name) {
        TypeInformation<ArrayList<Last>> listType = (TypeInformation) Types.LIST(TypeInformation.of(Last.class));
        bySecond = rc.getMapState(new MapStateDescriptor<>(name + "-by-second", Types.LONG, listType));
        newest = rc.getState(new ValueStateDescriptor<>(name + "-newest", Long.class));
        oldest = rc.getState(new ValueStateDescriptor<>(name + "-oldest", Long.class));
      }

      void add(Last l) throws Exception {
        long sec = Math.floorDiv(l.tsMs, 1000);
        ArrayList<Last> b = bySecond.get(sec);
        b = b == null ? new ArrayList<>() : new ArrayList<>(b);
        int i = b.size();
        while (i > 0 && (b.get(i - 1).tsMs > l.tsMs || (b.get(i - 1).tsMs == l.tsMs && b.get(i - 1).tradeId > l.tradeId))) {
          i--;
        }
        b.add(i, l);
        bySecond.put(sec, b);
        if (newest.value() == null || l.tsMs > newest.value()) {
          newest.update(l.tsMs);
        }
        if (oldest.value() == null || sec < oldest.value()) {
          oldest.update(sec);
        }
      }

      /** Latest trade at or before tick, looking back at most maxStaleMs; null if none. */
      Last asOf(long tick, long maxStaleMs) throws Exception {
        long floor = Math.floorDiv(tick - maxStaleMs, 1000);
        Long old = oldest.value();
        if (old != null) {
          floor = Math.max(floor, old);
        }
        for (long sec = Math.floorDiv(tick, 1000); sec >= floor; sec--) {
          ArrayList<Last> b = bySecond.get(sec);
          if (b == null) {
            continue;
          }
          for (int i = b.size() - 1; i >= 0; i--) {
            if (b.get(i).tsMs <= tick) {
              return b.get(i);
            }
          }
        }
        return null;
      }

      /** Drops seconds before {@code sec}. */
      void trimBefore(long sec) throws Exception {
        Long old = oldest.value();
        if (old == null || old >= sec) {
          return;
        }
        for (long s = old; s < sec; s++) {
          bySecond.remove(s);
        }
        oldest.update(sec);
      }
    }

    @Override
    public void open(Configuration parameters) {
      coinbase = new Venue(getRuntimeContext(), "coinbase");
      kraken = new Venue(getRuntimeContext(), "kraken");
      nextTick = getRuntimeContext().getState(new ValueStateDescriptor<>("next-tick", Long.class));
      lastSampled = getRuntimeContext().getState(new ValueStateDescriptor<>("last-sampled", Long.class));
    }

    @Override
    public void processElement(TradeEvent t, Context ctx, Collector<Spread> out) throws Exception {
      Venue v = "coinbase".equals(t.venue) ? coinbase : "kraken".equals(t.venue) ? kraken : null;
      if (v == null || t.price == null) {
        return;
      }
      Last l = new Last();
      l.price = t.price;
      l.tsMs = t.eventTsUs / 1000;
      l.tradeId = t.tradeId;
      v.add(l);
      long tick = (Math.floorDiv(l.tsMs, 1000) + 1) * 1000; // next whole second
      Long done = lastSampled.value();
      Long scheduled = nextTick.value();
      if ((done == null || tick > done) && (scheduled == null || tick < scheduled)) {
        ctx.timerService().registerEventTimeTimer(tick);
        nextTick.update(tick);
      }
      Long cb = coinbase.newest.value();
      Long kr = kraken.newest.value();
      if (cb != null && kr != null) {
        advance(Math.min(cb, kr) - latenessMs, ctx.getCurrentKey(), ctx.timerService(), out);
      }
    }

    @Override
    public void onTimer(long tick, OnTimerContext ctx, Collector<Spread> out) throws Exception {
      advance(tick, ctx.getCurrentKey(), ctx.timerService(), out); // the watermark has passed tick
    }

    /** Samples every pending tick up to {@code upTo}, then trims history no later tick can need. */
    private void advance(long upTo, String product, org.apache.flink.streaming.api.TimerService timers,
        Collector<Spread> out) throws Exception {
      Long pending = nextTick.value();
      if (pending == null || pending > upTo) {
        return;
      }
      long tick = pending;
      long newest = Math.max(orMin(coinbase.newest.value()), orMin(kraken.newest.value()));
      while (tick <= upTo) {
        Last c = coinbase.asOf(tick, maxStaleMs);
        Last k = kraken.asOf(tick, maxStaleMs);
        if (c != null && k != null && tick - c.tsMs <= maxStaleMs && tick - k.tsMs <= maxStaleMs) {
          out.collect(of(product, tick, c, k));
        }
        tick += 1000;
        if (newest < tick - maxStaleMs) {
          break; // nothing fresh at the next tick on either venue: stop ticking until a trade arrives
        }
      }
      lastSampled.update(tick - 1000);
      if (newest >= tick - maxStaleMs) {
        timers.registerEventTimeTimer(tick); // backstop if trades stop arriving
        nextTick.update(tick);
      } else {
        nextTick.clear();
      }
      long keepFrom = Math.floorDiv(tick - maxStaleMs, 1000) - 1;
      coinbase.trimBefore(keepFrom);
      kraken.trimBefore(keepFrom);
    }

    private static long orMin(Long v) {
      return v == null ? Long.MIN_VALUE : v;
    }

    static Spread of(String product, long tick, Last cb, Last kr) {
      Spread s = new Spread();
      s.productId = product;
      s.tsMs = tick;
      s.coinbasePrice = cb.price;
      s.krakenPrice = kr.price;
      s.coinbaseAgeMs = tick - cb.tsMs;
      s.krakenAgeMs = tick - kr.tsMs;
      s.spread = cb.price.subtract(kr.price);
      BigDecimal mid = cb.price.add(kr.price).divide(BigDecimal.valueOf(2), MathContext.DECIMAL64);
      s.spreadBps = mid.signum() == 0 ? 0 : s.spread.divide(mid, new MathContext(20, RoundingMode.HALF_EVEN))
          .multiply(BigDecimal.valueOf(10_000)).doubleValue();
      return s;
    }
  }
}
