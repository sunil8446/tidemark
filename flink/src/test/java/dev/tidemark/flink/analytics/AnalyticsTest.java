package dev.tidemark.flink.analytics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.tidemark.flink.model.TradeEvent;
import java.math.BigDecimal;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class AnalyticsTest {

  static TradeEvent t(String venue, long id, String price, String size, long tsMs, String side) {
    TradeEvent e = new TradeEvent();
    e.venue = venue;
    e.productId = "BTC-USD";
    e.tradeId = id;
    e.price = new BigDecimal(price);
    e.size = new BigDecimal(size);
    e.eventTsUs = tsMs * 1000;
    e.takerSide = side;
    return e;
  }

  @Test
  void barUsesEventTimeOrderNotArrivalOrder() {
    BarAccumulator a = new BarAccumulator();
    a.add(t("coinbase", 3, "103", "1", 3000, "buy"));
    a.add(t("coinbase", 1, "100", "2", 1000, "sell")); // late arrival, earliest trade
    a.add(t("coinbase", 2, "105", "1", 2000, "buy"));
    assertEquals(new BigDecimal("100"), a.open);
    assertEquals(new BigDecimal("103"), a.close);
    assertEquals(new BigDecimal("105"), a.high);
    assertEquals(new BigDecimal("100"), a.low);
    assertEquals(new BigDecimal("4"), a.volume);
    assertEquals(0, new BigDecimal("102").compareTo(a.vwap())); // (100*2+105+103)/4
    assertEquals(new BigDecimal("2"), a.buyVolume);
    assertEquals(3, a.count);
  }

  @Test
  void mergeIsOrderIndependent() {
    BarAccumulator x = new BarAccumulator();
    x.add(t("kraken", 1, "10", "1", 1000, "buy"));
    BarAccumulator y = new BarAccumulator();
    y.add(t("kraken", 2, "12", "1", 2000, "sell"));
    BarAccumulator xy = new BarAccumulator().merge(x).merge(y);
    BarAccumulator yx = new BarAccumulator().merge(y).merge(x);
    assertEquals(xy.open, yx.open);
    assertEquals(xy.close, yx.close);
    assertEquals(new BigDecimal("10"), xy.open);
    assertEquals(new BigDecimal("12"), xy.close);
  }

  @Test
  void spreadSamplesEverySecondAndNeverReportsNegativeAge() throws Exception {
    try (var h = new KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, Spread>(
        new KeyedProcessOperator<>(new Spread.Fn(60_000)), e -> e.productId, Types.STRING)) {
      h.open();
      h.processElement(new StreamRecord<>(t("coinbase", 1, "100.0", "1", 10_200, "buy"), 10_200));
      h.processElement(new StreamRecord<>(t("kraken", 1, "99.0", "1", 10_500, "buy"), 10_500));
      // A trade after the 11 s tick arrives before the watermark passes 11 s.
      h.processElement(new StreamRecord<>(t("coinbase", 2, "101.0", "1", 11_300, "buy"), 11_300));
      h.processWatermark(12_000);
      List<Spread> out = h.extractOutputValues();
      assertEquals(2, out.size()); // ticks at 11 s and 12 s
      // As-of 11 s: coinbase 100.0 (t=10.2 s), kraken 99.0 (t=10.5 s); the 11.3 s trade must not leak in.
      Spread first = out.get(0);
      assertEquals(11_000, first.tsMs);
      assertEquals(800, first.coinbaseAgeMs);
      assertEquals(500, first.krakenAgeMs);
      assertEquals(0, new BigDecimal("1.0").compareTo(first.spread));
      assertEquals(100.50, first.spreadBps, 0.01); // 1 / 99.5 * 10_000
      // As-of 12 s the later coinbase trade applies.
      Spread second = out.get(1);
      assertEquals(0, new BigDecimal("2.0").compareTo(second.spread));
      assertEquals(700, second.coinbaseAgeMs);
    }
  }

  @Test
  void staleVenueSuppressesSpread() throws Exception {
    try (var h = new KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, Spread>(
        new KeyedProcessOperator<>(new Spread.Fn(5_000)), e -> e.productId, Types.STRING)) {
      h.open();
      h.processElement(new StreamRecord<>(t("kraken", 1, "99", "1", 1_000, "buy"), 1_000));
      h.processElement(new StreamRecord<>(t("coinbase", 1, "100", "1", 20_000, "buy"), 20_000));
      h.processWatermark(21_000);
      assertTrue(h.extractOutputValues().isEmpty(), "kraken price is 20 s old; no spread expected");
    }
  }
}
