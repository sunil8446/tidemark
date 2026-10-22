package dev.tidemark.flink.model;

/** One gap lifecycle event (DETECTED, then a terminal status). */
public class GapRecord {
  public String gapId;
  public String venue;
  public String productId;
  public String kind;
  public String status;
  public long firstMissingTradeId;
  public long lastMissingTradeId;
  public long recoveredCount;
  public long detectedTsUs;
  public long resolvedTsUs; // 0 while open
  public String detail;
  public int kafkaPartition;
  public long kafkaOffset;

  public GapRecord() {}
}
