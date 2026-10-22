package dev.tidemark.flink.detect;

import dev.tidemark.flink.model.TradeEvent;
import java.math.BigDecimal;
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
 * Volume spike per venue and product.
 *
 * <p>Notional is collected in 1 s event-time buckets. Every second the sum of
 * the last {@code windowS} seconds is scored against a rolling baseline of
 * such sums ({@code baselineS} samples, 30 min by default), so a burst is
 * caught however it aligns with clock boundaries.
 *
 * <p>The score discounts price impact: {@code z / (1 + rangeBps / impactBps)},
 * where rangeBps is the high-low range of the window's trades. Evaluation on
 * live data showed real volume bursts are mostly aggressive sweeps that move
 * the price, while volume inflation (wash-style) prints at the prevailing
 * price; raw z alone could not tell them apart within a sane alert budget.
 *
 * <p>An episode starts when the baseline is warm, score >= threshold, the window
 * holds at least {@code minNotional}, and it is at least {@code minMultiple}
 * times the baseline mean. The first alert goes out immediately; if the
 * episode keeps growing, the same alert id is re-emitted with the higher
 * score, so the stored score is the episode peak. Episode samples are kept
 * out of the baseline so a long spike does not normalise itself.
 */
public class VolumeSpike extends KeyedProcessFunction<String, TradeEvent, AlertEvent> {
  public static final String NAME = "volume_spike";

  private final int windowS;
  private final int baselineS;
  private final int minBaseline;
  private final double zThreshold;
  private final double minMultiple;
  private final double minNotional;
  private final double impactBps;

  private transient MapState<Long, double[]> open; // second start ms -> [notional, low, high]
  private transient ValueState<double[]> window; // ring of the last windowS seconds, 3 values each
  private transient ValueState<RollingStats> baseline;
  private transient ValueState<Long> lastClosed;
  private transient ValueState<double[]> episode; // [startMs, peakZ, peakNotional]

  public VolumeSpike(int windowS, int baselineS, int minBaseline, double zThreshold, double minMultiple,
      double minNotional, double impactBps) {
    this.impactBps = impactBps;
    this.windowS = windowS;
    this.baselineS = baselineS;
    this.minBaseline = minBaseline;
    this.zThreshold = zThreshold;
    this.minMultiple = minMultiple;
    this.minNotional = minNotional;
  }

  @Override
  public void open(Configuration parameters) {
    open = getRuntimeContext().getMapState(new MapStateDescriptor<>("open", Types.LONG, TypeInformation.of(double[].class)));
    window = getRuntimeContext().getState(new ValueStateDescriptor<>("window", double[].class));
    baseline = getRuntimeContext().getState(new ValueStateDescriptor<>("baseline", TypeInformation.of(RollingStats.class)));
    lastClosed = getRuntimeContext().getState(new ValueStateDescriptor<>("last-closed", Long.class));
    episode = getRuntimeContext().getState(new ValueStateDescriptor<>("episode", double[].class));
  }

  @Override
  public void processElement(TradeEvent t, Context ctx, Collector<AlertEvent> out) throws Exception {
    if (t.price == null || t.size == null) {
      return;
    }
    long ts = t.eventTsUs / 1000;
    long sec = ts - Math.floorMod(ts, 1000L);
    Long closed = lastClosed.value();
    if (closed != null && sec <= closed) {
      return; // later than its second's close; the lakehouse still has it
    }
    double[] cur = open.get(sec);
    double px = t.price.doubleValue();
    if (cur == null) {
      cur = new double[] {0, px, px};
      ctx.timerService().registerEventTimeTimer(sec + 999);
    }
    cur[0] += t.price.multiply(t.size).doubleValue();
    cur[1] = Math.min(cur[1], px);
    cur[2] = Math.max(cur[2], px);
    open.put(sec, cur);
  }

  @Override
  public void onTimer(long ts, OnTimerContext ctx, Collector<AlertEvent> out) throws Exception {
    long sec = ts - 999;
    double[] agg = open.get(sec);
    if (agg == null) {
      return;
    }
    open.remove(sec);
    Long prev = lastClosed.value();
    if (prev != null && sec - prev > (long) baselineS * 1000) {
      prev = sec - (long) baselineS * 1000; // long silence: refill at most one baseline of zeros
    }
    if (prev != null) {
      for (long s = prev + 1000; s < sec; s += 1000) {
        step(s, new double[] {0, 0, 0}, ctx.getCurrentKey(), out);
      }
    }
    step(sec, agg, ctx.getCurrentKey(), out);
    lastClosed.update(sec);
  }

  private void step(long sec, double[] agg, String key, Collector<AlertEvent> out) throws Exception {
    double[] w = window.value();
    if (w == null) {
      w = new double[windowS * 3];
    }
    int slot = (int) ((sec / 1000) % windowS) * 3;
    System.arraycopy(agg, 0, w, slot, 3);
    window.update(w);
    double sum = 0;
    double lo = Double.MAX_VALUE;
    double hi = 0;
    for (int i = 0; i < windowS; i++) {
      sum += w[i * 3];
      if (w[i * 3] > 0) {
        lo = Math.min(lo, w[i * 3 + 1]);
        hi = Math.max(hi, w[i * 3 + 2]);
      }
    }
    double rangeBps = hi > 0 ? (hi - lo) / ((hi + lo) / 2) * 10_000 : 0;
    RollingStats b = baseline.value();
    if (b == null) {
      b = new RollingStats(baselineS);
    }
    double mean = b.mean();
    double z = b.z(sum);
    double score = Math.min(z, 1e6) / (1 + rangeBps / impactBps);
    boolean hot = b.count >= minBaseline && score >= zThreshold && sum >= minNotional && sum >= minMultiple * mean;
    double[] ep = episode.value();
    if (hot) {
      if (ep == null || score > ep[1]) {
        long start = ep == null ? sec - (windowS - 1) * 1000L : (long) ep[0];
        episode.update(new double[] {start, score, Math.max(sum, ep == null ? 0 : ep[2])});
        out.collect(alert(key, start, sec + 1000, score, z, rangeBps, sum, mean, b));
      }
    } else {
      episode.clear();
      // A high-volume window that moved price stays out of the baseline too,
      // but every sample counts while the baseline is still warming up.
      if (b.count < minBaseline || z < zThreshold) {
        b.add(sum);
        baseline.update(b);
      }
    }
  }

  private AlertEvent alert(String key, long start, long end, double score, double z, double rangeBps, double sum,
      double mean, RollingStats b) {
    int sep = key.indexOf(':');
    AlertEvent a = AlertEvent.of(NAME, key.substring(0, sep), key.substring(sep + 1), start, end, end, score, zThreshold);
    a.summary = String.format("%s %s traded %.0f notional in %d s (%.1f sigma above its 30-min baseline, mean %.0f) while price moved only %.1f bps",
        a.venue, a.productId, sum, windowS, z, mean, rangeBps);
    a.put("window_notional", BigDecimal.valueOf(sum).toPlainString())
        .put("volume_z", z)
        .put("price_range_bps", rangeBps)
        .put("window_s", windowS)
        .put("baseline_mean", mean)
        .put("baseline_std", b.std())
        .put("baseline_samples", b.count)
        .put("multiple_of_mean", mean == 0 ? "inf" : String.valueOf(sum / mean));
    return a;
  }
}
