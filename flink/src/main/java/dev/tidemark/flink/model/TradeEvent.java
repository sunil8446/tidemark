package dev.tidemark.flink.model;

import java.math.BigDecimal;

/**
 * A trade as a Flink POJO. Protobuf classes are converted at the source so
 * Flink uses its own POJO serializer rather than Kryo.
 */
public class TradeEvent {
  public String venue;
  public String productId;
  public long tradeId;
  public BigDecimal price;
  public BigDecimal size;
  public String takerSide;
  public long eventTsUs;
  public long recvTsUs;
  public String source;
  public String ingestId;
  public boolean fromSnapshot;
  public int kafkaPartition;
  public long kafkaOffset;

  public TradeEvent() {}

  /** Dedup key: one record per venue, product and trade id. */
  public String tradeKey() {
    return venue + ":" + productId + ":" + tradeId;
  }

  public String instrumentKey() {
    return venue + ":" + productId;
  }

  @Override
  public String toString() {
    return "TradeEvent{" + tradeKey() + " " + price + " x " + size + " @" + eventTsUs + " " + source + "}";
  }
}
