package dev.tidemark.flink.detect;

import dev.tidemark.flink.model.TradeEvent;
import java.time.Duration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Wash trading per venue and product. Needs account identity, which public
 * feeds do not carry, so it only runs on streams where the injection framework
 * assigned synthetic accounts.
 *
 * <ul>
 *   <li><b>self trade:</b> buyer and seller are the same account (score 1).
 *   <li><b>round trip:</b> A sells to B, then B sells the same size back to A
 *       within {@code windowMs} at nearly the same price. Score is
 *       1 - max(relative size difference, relative price difference).
 * </ul>
 */
public class WashTrade extends KeyedProcessFunction<String, TradeEvent, AlertEvent> {
  public static final String NAME = "wash_trade";

  private final long windowMs;
  private final double minScore;

  private transient MapState<String, Leg> legs; // "seller>buyer" -> last trade

  public static class Leg {
    public long tradeId;
    public long tsMs;
    public double size;
    public double price;

    public Leg() {}
  }

  public WashTrade(long windowMs, double minScore) {
    this.windowMs = windowMs;
    this.minScore = minScore;
  }

  @Override
  public void open(Configuration parameters) {
    MapStateDescriptor<String, Leg> d = new MapStateDescriptor<>("legs", Types.STRING, TypeInformation.of(Leg.class));
    d.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofMillis(windowMs * 2)).cleanupFullSnapshot().build());
    legs = getRuntimeContext().getMapState(d);
  }

  @Override
  public void processElement(TradeEvent t, Context ctx, Collector<AlertEvent> out) throws Exception {
    if (t.buyerAccount == null || t.sellerAccount == null || t.price == null || t.size == null) {
      return;
    }
    long ts = t.eventTsUs / 1000;
    String key = ctx.getCurrentKey();
    int sep = key.indexOf(':');
    String venue = key.substring(0, sep);
    String product = key.substring(sep + 1);

    if (t.buyerAccount.equals(t.sellerAccount)) {
      AlertEvent a = AlertEvent.of(NAME, venue, product, ts, ts, ts, 1.0, minScore);
      a.alertId = a.alertId + ":" + t.tradeId;
      a.summary = String.format("%s %s: account %s traded %s with itself at %s",
          venue, product, t.buyerAccount, t.size.toPlainString(), t.price.toPlainString());
      a.put("kind", "self_trade").put("account", t.buyerAccount).put("trade_id", t.tradeId)
          .put("size", t.size.toPlainString()).put("price", t.price.toPlainString());
      out.collect(a);
      return;
    }

    double size = t.size.doubleValue();
    double price = t.price.doubleValue();
    Leg back = legs.get(t.buyerAccount + ">" + t.sellerAccount); // the opposite leg: buyer sold to seller earlier
    if (back != null && ts - back.tsMs <= windowMs && ts >= back.tsMs) {
      double sizeDiff = Math.abs(size - back.size) / Math.max(size, back.size);
      double priceDiff = Math.abs(price - back.price) / Math.max(price, back.price);
      double score = 1 - Math.max(sizeDiff, priceDiff);
      if (score >= minScore) {
        AlertEvent a = AlertEvent.of(NAME, venue, product, back.tsMs, ts, ts, score, minScore);
        a.alertId = a.alertId + ":" + back.tradeId + ":" + t.tradeId;
        a.summary = String.format("%s %s: %s and %s traded %.6f back and forth within %d ms (size diff %.2f%%, price diff %.3f%%)",
            venue, product, t.buyerAccount, t.sellerAccount, size, ts - back.tsMs, sizeDiff * 100, priceDiff * 100);
        a.put("kind", "round_trip").put("account_a", t.buyerAccount).put("account_b", t.sellerAccount)
            .put("first_trade_id", back.tradeId).put("second_trade_id", t.tradeId)
            .put("size_diff", sizeDiff).put("price_diff", priceDiff).put("gap_ms", ts - back.tsMs);
        out.collect(a);
        legs.remove(t.buyerAccount + ">" + t.sellerAccount);
        return;
      }
    }
    Leg leg = new Leg();
    leg.tradeId = t.tradeId;
    leg.tsMs = ts;
    leg.size = size;
    leg.price = price;
    legs.put(t.sellerAccount + ">" + t.buyerAccount, leg);
  }
}
