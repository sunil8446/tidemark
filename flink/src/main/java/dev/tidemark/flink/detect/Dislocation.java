package dev.tidemark.flink.detect;

import dev.tidemark.flink.analytics.Spread;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Cross-venue dislocation per product: the 1 s spread sample is scored against
 * a rolling baseline of {@code baselineSamples} samples (30 min at 1 Hz). An
 * episode fires when |z| >= threshold and |spread| >= minBps hold for
 * {@code sustain} consecutive samples. The score is the smallest |z| over the
 * sustained run, so one outlier sample cannot carry it.
 *
 * <p>Only samples where both venues traded within {@code maxAgeMs} count:
 * evaluation showed most high-z false positives were one venue's stale last
 * price, not a real dislocation. As an episode grows, the same alert id is
 * re-emitted with the higher score, so the stored score is the best sustained
 * |z| of the episode.
 */
public class Dislocation extends KeyedProcessFunction<String, Spread, AlertEvent> {
  public static final String NAME = "dislocation";

  private final int baselineSamples;
  private final int minBaseline;
  private final double zThreshold;
  private final double minBps;
  private final int sustain;
  private final long maxAgeMs;

  private transient ValueState<RollingStats> baseline;
  private transient ValueState<double[]> run;

  public Dislocation(int baselineSamples, int minBaseline, double zThreshold, double minBps, int sustain,
      long maxAgeMs) {
    this.maxAgeMs = maxAgeMs;
    this.baselineSamples = baselineSamples;
    this.minBaseline = minBaseline;
    this.zThreshold = zThreshold;
    this.minBps = minBps;
    this.sustain = sustain;
  }

  @Override
  public void open(Configuration parameters) {
    baseline = getRuntimeContext().getState(new ValueStateDescriptor<>("baseline", TypeInformation.of(RollingStats.class)));
    run = getRuntimeContext().getState(new ValueStateDescriptor<>("run", double[].class));
  }

  @Override
  public void processElement(Spread s, Context ctx, Collector<AlertEvent> out) throws Exception {
    if (s.coinbaseAgeMs > maxAgeMs || s.krakenAgeMs > maxAgeMs) {
      run.clear(); // stale sample: neither evidence nor baseline
      return;
    }
    RollingStats b = baseline.value();
    if (b == null) {
      b = new RollingStats(baselineSamples);
    }
    double z = b.z(s.spreadBps);
    boolean outside = b.count >= minBaseline && Math.abs(z) >= zThreshold && Math.abs(s.spreadBps) >= minBps;
    double[] r = run.value(); // [length, startMs, sign, emittedScore, maxAbsBps, last-sustain |z| values...]
    if (outside && (r == null || Math.signum(z) == r[2])) {
      if (r == null) {
        r = new double[5 + sustain];
        r[1] = s.tsMs;
        r[2] = Math.signum(z);
      }
      r[5 + (int) (r[0] % sustain)] = Math.abs(z);
      r[0] += 1;
      r[4] = Math.max(r[4], Math.abs(s.spreadBps));
      if (r[0] >= sustain) {
        double sustained = Double.MAX_VALUE;
        for (int i = 0; i < sustain; i++) {
          sustained = Math.min(sustained, r[5 + i]);
        }
        if (sustained > r[3]) {
          r[3] = sustained;
          AlertEvent a = AlertEvent.of(NAME, "", s.productId, (long) r[1], s.tsMs, s.tsMs, sustained, zThreshold);
          a.summary = String.format("%s Coinbase-Kraken spread %+.2f bps, at least %.1f sigma from its baseline for %d s (baseline mean %.2f bps)",
              s.productId, s.spreadBps, sustained, (long) r[0], b.mean());
          a.put("spread_bps", s.spreadBps).put("max_abs_bps", r[4]).put("coinbase_price", s.coinbasePrice.toPlainString())
              .put("kraken_price", s.krakenPrice.toPlainString()).put("coinbase_age_ms", s.coinbaseAgeMs)
              .put("kraken_age_ms", s.krakenAgeMs).put("baseline_mean_bps", b.mean())
              .put("baseline_std_bps", b.std()).put("sustained_s", (long) r[0]);
          out.collect(a);
        }
      }
      run.update(r);
    } else {
      run.clear();
      // Episodes stay out of the baseline so a long dislocation does not normalise itself.
      if (!outside) {
        b.add(s.spreadBps);
        baseline.update(b);
      }
    }
  }
}
