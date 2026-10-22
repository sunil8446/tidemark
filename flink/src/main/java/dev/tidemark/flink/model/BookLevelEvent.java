package dev.tidemark.flink.model;

import java.math.BigDecimal;

/** One price level from a book snapshot or update, flattened for the lakehouse. */
public class BookLevelEvent {
  public String venue;
  public String productId;
  public long seq;
  public boolean isSnapshot;
  public String side; // "bid" or "ask"
  public int levelIndex; // position within the message
  public BigDecimal price;
  public BigDecimal qty; // 0 removes the level
  public long checksum;
  public boolean checksumOk;
  public long eventTsUs;
  public long recvTsUs;
  public String source;
  public int kafkaPartition;
  public long kafkaOffset;

  public BookLevelEvent() {}
}
