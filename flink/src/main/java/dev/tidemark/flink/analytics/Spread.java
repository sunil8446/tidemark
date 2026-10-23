package dev.tidemark.flink.analytics;

import dev.tidemark.flink.model.TradeEvent;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
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
 * <p>Ticks are sampled in <b>trade time</b>: once a trade arrives more than
 * {@code latenessMs} past a tick, that tick is final and is sampled right away.
 * Event-time timers are only a backstop for a product that goes quiet. An
 * earlier version sampled only on timers; while the watermark lagged (catch-up
 * after downtime, where the other input of the detectors job held it back) the
 * history was never trimmed, grew to thousands of trades, and every new trade
 * copied the whole list: quadratic, and the job stopped checkpointing.
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
    private transient ValueState<ArrayList<Last>> coinbase;
    private transient ValueState<ArrayList<Last>> kraken;
    private transient ValueState<Long> nextTick;
    private transient ValueState<Long> lastSampled;

    public Fn(long maxStaleMs) {
      this(maxStaleMs, 5_000);
    }

    /** @param latenessMs how far trade time must pass a tick before it is final (the source's out-of-orderness). */
    public Fn(long maxStaleMs, long latenessMs) {
      this.maxStaleMs = maxStaleMs;
      this.latenessMs = latenessMs;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void open(Configuration parameters) {
      TypeInformation<ArrayList<Last>> listType = (TypeInformation) Types.LIST(TypeInformation.of(Last.class));
      coinbase = getRuntimeContext().getState(new ValueStateDescriptor<>("coinbase", listType));
      kraken = getRuntimeContext().getState(new ValueStateDescriptor<>("kraken", listType));
      nextTick = getRuntimeContext().getState(new ValueStateDescriptor<>("next-tick", Long.class));
      lastSampled = getRuntimeContext().getState(new ValueStateDescriptor<>("last-sampled", Long.class));
    }

    @Override
    public void processElement(TradeEvent t, Context ctx, Collector<Spread> out) throws Exception {
      ValueState<ArrayList<Last>> s = "coinbase".equals(t.venue) ? coinbase : "kraken".equals(t.venue) ? kraken : null;
      if (s == null || t.price == null) {
        return;
      }
      Last l = new Last();
      l.price = t.price;
      l.tsMs = t.eventTsUs / 1000;
      l.tradeId = t.tradeId;
      ArrayList<Last> hist = s.value() == null ? new ArrayList<>() : new ArrayList<>(s.value());
      int i = hist.size();
      while (i > 0 && (hist.get(i - 1).tsMs > l.tsMs
          || (hist.get(i - 1).tsMs == l.tsMs && hist.get(i - 1).tradeId > l.tradeId))) {
        i--;
      }
      hist.add(i, l);
      s.update(hist);
      long tick = (l.tsMs / 1000 + 1) * 1000; // next whole second
      Long done = lastSampled.value();
      Long scheduled = nextTick.value();
      if ((done == null || tick > done) && (scheduled == null || tick < scheduled)) {
        ctx.timerService().registerEventTimeTimer(tick);
        nextTick.update(tick);
      }
      // Sample every tick that trade time has moved past by more than the allowed lateness.
      long finalUpTo = Math.max(newest(coinbase), newest(kraken)) - latenessMs;
      while (nextTick.value() != null && nextTick.value() <= finalUpTo) {
        sample(nextTick.value(), ctx.getCurrentKey(), ctx.timerService(), out);
      }
    }

    /** Latest entry at or before tick; drops entries older than it. Null if none. */
    static Last asOf(ValueState<ArrayList<Last>> s, long tick) throws Exception {
      ArrayList<Last> hist = s.value();
      if (hist == null || hist.isEmpty()) {
        return null;
      }
      int idx = -1;
      for (int i = 0; i < hist.size() && hist.get(i).tsMs <= tick; i++) {
        idx = i;
      }
      if (idx > 0) {
        s.update(new ArrayList<>(hist.subList(idx, hist.size())));
      }
      return idx < 0 ? null : hist.get(idx);
    }

    static long newest(ValueState<ArrayList<Last>> s) throws Exception {
      ArrayList<Last> hist = s.value();
      return hist == null || hist.isEmpty() ? Long.MIN_VALUE : hist.get(hist.size() - 1).tsMs;
    }

    @Override
    public void onTimer(long tick, OnTimerContext ctx, Collector<Spread> out) throws Exception {
      Long scheduled = nextTick.value();
      if (scheduled == null || tick != scheduled) {
        return; // already sampled in trade time, or superseded
      }
      sample(tick, ctx.getCurrentKey(), ctx.timerService(), out);
    }

    private void sample(long tick, String product, org.apache.flink.streaming.api.TimerService timers,
        Collector<Spread> out) throws Exception {
      Last cb = asOf(coinbase, tick);
      Last kr = asOf(kraken, tick);
      if (cb != null && kr != null && tick - cb.tsMs <= maxStaleMs && tick - kr.tsMs <= maxStaleMs) {
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
        out.collect(s);
      }
      lastSampled.update(tick);
      // Keep ticking while either venue has a price that will still be fresh at the next tick.
      long next = tick + 1000;
      if (Math.max(newest(coinbase), newest(kraken)) >= next - maxStaleMs) {
        timers.registerEventTimeTimer(next); // backstop if trades stop arriving
        nextTick.update(next);
      } else {
        nextTick.clear();
      }
    }
  }
}
