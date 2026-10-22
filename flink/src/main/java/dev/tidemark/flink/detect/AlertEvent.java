package dev.tidemark.flink.detect;

import java.util.LinkedHashMap;
import java.util.Map;

/** A detector firing, as a Flink POJO (mirrors the Alert Protobuf message). */
public class AlertEvent {
  public String alertId;
  public String detector;
  public String venue; // "" for cross-venue
  public String productId;
  public long eventTsMs;
  public long windowStartMs;
  public long windowEndMs;
  public double score;
  public double threshold;
  public String severity;
  public String summary;
  public Map<String, String> evidence = new LinkedHashMap<>();
  public long emittedTsMs;

  public AlertEvent() {}

  /** Deterministic id: replays of the same firing upsert rather than duplicate. */
  public static AlertEvent of(String detector, String venue, String product, long windowStartMs, long windowEndMs,
      long eventTsMs, double score, double threshold) {
    AlertEvent a = new AlertEvent();
    a.detector = detector;
    a.venue = venue == null ? "" : venue;
    a.productId = product;
    a.windowStartMs = windowStartMs;
    a.windowEndMs = windowEndMs;
    a.eventTsMs = eventTsMs;
    a.score = score;
    a.threshold = threshold;
    a.alertId = detector + ":" + (a.venue.isEmpty() ? "x" : a.venue) + ":" + product + ":" + windowStartMs;
    double ratio = threshold == 0 ? 1 : score / threshold;
    a.severity = ratio >= 2 ? "high" : ratio >= 1.3 ? "medium" : "low";
    return a;
  }

  public AlertEvent put(String k, Object v) {
    evidence.put(k, String.valueOf(v));
    return this;
  }
}
