package dev.tidemark.flink.detect;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

/**
 * Stamps emitted_at and records end-to-end alert latency: the event time that
 * triggered the alert to the moment it leaves the detector. Exposed per
 * detector as {@code tidemark_alerts_total}, {@code tidemark_alerts_within_slo_total}
 * (latency <= sloMs, the SLI) and a latency histogram for p50/p99.
 * Only the live stream counts; replays carry hours-old event times.
 */
public class StampAlerts extends RichMapFunction<AlertEvent, AlertEvent> {
  private final boolean live;
  private final long sloMs;
  private transient Map<String, Counter[]> counters;
  private transient Map<String, Histogram> latency;
  private transient MetricGroup group;

  public StampAlerts(boolean live, long sloMs) {
    this.live = live;
    this.sloMs = sloMs;
  }

  @Override
  public void open(Configuration parameters) {
    counters = new HashMap<>();
    latency = new HashMap<>();
    group = getRuntimeContext().getMetricGroup();
  }

  @Override
  public AlertEvent map(AlertEvent a) {
    a.emittedTsMs = System.currentTimeMillis();
    if (live) {
      long ms = a.emittedTsMs - a.eventTsMs;
      Counter[] c = counters.computeIfAbsent(a.detector, d -> {
        MetricGroup g = group.addGroup("detector", d);
        return new Counter[] {g.counter("tidemark_alerts_total"), g.counter("tidemark_alerts_within_slo_total")};
      });
      c[0].inc();
      if (ms <= sloMs) {
        c[1].inc();
      }
      latency.computeIfAbsent(a.detector, d -> group.addGroup("detector", d)
          .histogram("tidemark_alert_latency_ms", new DescriptiveStatisticsHistogram(1_000))).update(ms);
    }
    return a;
  }
}
