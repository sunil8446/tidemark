package dev.tidemark.flink.analytics;

import java.math.BigDecimal;

/** An OHLCV bar for one venue and product over one interval. */
public class Bar {
  public String venue;
  public String productId;
  public String interval; // "1s", "1m"
  public long barStartMs;
  public BigDecimal open;
  public BigDecimal high;
  public BigDecimal low;
  public BigDecimal close;
  public BigDecimal volume;
  public BigDecimal notional;
  public BigDecimal vwap;
  public BigDecimal buyVolume;
  public BigDecimal sellVolume;
  public long tradeCount;
  /** Window firings for this bar so far; above 1 means late trades corrected it. */
  public long revision;

  public Bar() {}
}
