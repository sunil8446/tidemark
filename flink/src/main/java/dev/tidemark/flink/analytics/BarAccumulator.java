package dev.tidemark.flink.analytics;

import dev.tidemark.flink.model.TradeEvent;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental OHLCV state. Open and close are chosen by (event time, trade id)
 * rather than arrival order, so late or backfilled trades land in the right
 * place when the window re-fires.
 */
public class BarAccumulator {
  public BigDecimal open;
  public long openTs = Long.MAX_VALUE;
  public long openId = Long.MAX_VALUE;
  public BigDecimal close;
  public long closeTs = Long.MIN_VALUE;
  public long closeId = Long.MIN_VALUE;
  public BigDecimal high;
  public BigDecimal low;
  public BigDecimal volume = BigDecimal.ZERO;
  public BigDecimal notional = BigDecimal.ZERO;
  public BigDecimal buyVolume = BigDecimal.ZERO;
  public BigDecimal sellVolume = BigDecimal.ZERO;
  public long count;

  public BarAccumulator() {}

  static final MathContext VWAP_MC = new MathContext(28, RoundingMode.HALF_EVEN);

  public void add(TradeEvent t) {
    if (t.price == null || t.size == null) {
      return;
    }
    if (t.eventTsUs < openTs || (t.eventTsUs == openTs && t.tradeId < openId)) {
      open = t.price;
      openTs = t.eventTsUs;
      openId = t.tradeId;
    }
    if (t.eventTsUs > closeTs || (t.eventTsUs == closeTs && t.tradeId > closeId)) {
      close = t.price;
      closeTs = t.eventTsUs;
      closeId = t.tradeId;
    }
    high = high == null || t.price.compareTo(high) > 0 ? t.price : high;
    low = low == null || t.price.compareTo(low) < 0 ? t.price : low;
    volume = volume.add(t.size);
    notional = notional.add(t.price.multiply(t.size));
    if ("buy".equals(t.takerSide)) {
      buyVolume = buyVolume.add(t.size);
    } else if ("sell".equals(t.takerSide)) {
      sellVolume = sellVolume.add(t.size);
    }
    count++;
  }

  public BarAccumulator merge(BarAccumulator o) {
    if (o.count == 0) {
      return this;
    }
    if (count == 0) {
      return o;
    }
    BarAccumulator m = new BarAccumulator();
    boolean thisOpens = openTs < o.openTs || (openTs == o.openTs && openId <= o.openId);
    m.open = thisOpens ? open : o.open;
    m.openTs = thisOpens ? openTs : o.openTs;
    m.openId = thisOpens ? openId : o.openId;
    boolean thisCloses = closeTs > o.closeTs || (closeTs == o.closeTs && closeId >= o.closeId);
    m.close = thisCloses ? close : o.close;
    m.closeTs = thisCloses ? closeTs : o.closeTs;
    m.closeId = thisCloses ? closeId : o.closeId;
    m.high = high.max(o.high);
    m.low = low.min(o.low);
    m.volume = volume.add(o.volume);
    m.notional = notional.add(o.notional);
    m.buyVolume = buyVolume.add(o.buyVolume);
    m.sellVolume = sellVolume.add(o.sellVolume);
    m.count = count + o.count;
    return m;
  }

  public BigDecimal vwap() {
    return volume.signum() == 0 ? null : notional.divide(volume, VWAP_MC);
  }

  /** Flink AggregateFunction over trades. */
  public static class Agg implements AggregateFunction<TradeEvent, BarAccumulator, BarAccumulator> {
    @Override
    public BarAccumulator createAccumulator() {
      return new BarAccumulator();
    }

    @Override
    public BarAccumulator add(TradeEvent t, BarAccumulator acc) {
      acc.add(t);
      return acc;
    }

    @Override
    public BarAccumulator getResult(BarAccumulator acc) {
      return acc;
    }

    @Override
    public BarAccumulator merge(BarAccumulator a, BarAccumulator b) {
      return a.merge(b);
    }
  }
}
