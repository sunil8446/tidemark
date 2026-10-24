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

  @Test
  void spreadIsSampledInTradeTimeAndHistoryStaysSmallWithoutAWatermark() throws Exception {
    // The bug: with a lagging watermark no tick ever fired, the per-venue history grew without
    // bound and every trade re-copied it. Without any watermark, ticks must still be sampled once
    // trade time is past them by the lateness, exactly once each, and the history must stay short.
    try (var h = new KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, Spread>(
        new KeyedProcessOperator<>(new Spread.Fn(60_000, 5_000)), e -> e.productId, Types.STRING)) {
      h.open();
      for (int i = 0; i < 600; i++) { // 10 minutes of trades, one per second per venue, no watermark
        long ts = 1_000 + i * 1_000L + 200;
        h.processElement(new StreamRecord<>(t("coinbase", i, "100.0", "1", ts, "buy"), ts));
        h.processElement(new StreamRecord<>(t("kraken", i, "99.0", "1", ts + 100, "buy"), ts + 100));
      }
      List<Spread> out = h.extractOutputValues();
      // Ticks 2 s .. (last trade 600.3 s - 5 s lateness) are final: 2..595 s.
      assertEquals(594, out.size());
      assertEquals(2_000, out.get(0).tsMs);
      assertEquals(595_000, out.get(out.size() - 1).tsMs);
      for (int i = 1; i < out.size(); i++) {
        assertEquals(out.get(i - 1).tsMs + 1_000, out.get(i).tsMs); // each tick once, in order
      }
      // A late watermark afterwards must not re-emit ticks that were already sampled.
      h.processWatermark(590_000);
      assertEquals(594, h.extractOutputValues().size());
    }
  }

  @Test
  void catchUpAfterAHugeBacklogIsOnePassAndMatchesTickByTick() throws Exception {
    // State from a stalled run (no tick ever final): 3 hours of history and pending ticks.
    // Restored into the normal operator, the next trade must catch up in one pass, giving the
    // same as-of samples as ticking second by second, without quadratic time.
    int n = 3 * 3600;
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;
    try (var stalled = new KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, Spread>(
        new KeyedProcessOperator<>(new Spread.Fn(60_000, Long.MAX_VALUE / 4)), e -> e.productId, Types.STRING)) {
      stalled.open();
      for (int i = 0; i < n; i++) {
        long ts = 1_000 + i * 1_000L + 200;
        stalled.processElement(new StreamRecord<>(t("kraken", i, "99.0", "1", ts + 100, "buy"), ts + 100));
        stalled.processElement(new StreamRecord<>(
            t("coinbase", i, i % 2 == 0 ? "100.0" : "101.0", "1", ts, "buy"), ts));
      }
      assertTrue(stalled.extractOutputValues().isEmpty());
      snapshot = stalled.snapshot(1, 1);
    }
    try (var h = new KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, Spread>(
        new KeyedProcessOperator<>(new Spread.Fn(60_000, 5_000)), e -> e.productId, Types.STRING)) {
      h.initializeState(snapshot);
      h.open();
      long t0 = System.nanoTime();
      long last = 1_000 + n * 1_000L + 200;
      h.processElement(new StreamRecord<>(t("coinbase", n, "100.0", "1", last, "buy"), last));
      long ms = (System.nanoTime() - t0) / 1_000_000;
      List<Spread> out = h.extractOutputValues();
      // Final up to the slower venue (kraken, last at n s + 0.3 s) minus 5 s: ticks 2 s .. (n - 5) s.
      assertEquals(n - 6, out.size());
      for (Spread s : out) { // as-of: the coinbase price alternates every second
        long i = (s.tsMs - 1_200) / 1_000;
        assertEquals(0, new BigDecimal(i % 2 == 0 ? "100.0" : "101.0").compareTo(s.coinbasePrice), "tick " + s.tsMs);
      }
      assertTrue(ms < 5_000, "catch-up took " + ms + " ms");
    }
  }

  @Test
  void venuesFarApartInArrivalGiveTheSameSamplesAsInterleaved() throws Exception {
    // Coinbase and Kraken come through different partitions; in catch-up one can be minutes ahead.
    // A tick may only be final when both venues are past it, or the lagging venue's prices are
    // wrongly treated as stale (this lost 6 of 19 injected dislocations).
    java.util.function.Function<Boolean, List<Spread>> run = skewed -> {
      try (var h = new KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, Spread>(
          new KeyedProcessOperator<>(new Spread.Fn(30_000, 5_000)), e -> e.productId, Types.STRING)) {
        h.open();
        List<TradeEvent> cb = new java.util.ArrayList<>();
        List<TradeEvent> kr = new java.util.ArrayList<>();
        for (int i = 0; i < 600; i++) {
          long ts = 1_000 + i * 1_000L + 200;
          cb.add(t("coinbase", i, i % 3 == 0 ? "101.0" : "100.0", "1", ts, "buy"));
          kr.add(t("kraken", i, i % 5 == 0 ? "98.0" : "99.0", "1", ts + 300, "buy"));
        }
        if (skewed) { // all of coinbase, then all of kraken
          for (TradeEvent e : cb) {
            h.processElement(new StreamRecord<>(e, e.eventTsUs / 1000));
          }
          for (TradeEvent e : kr) {
            h.processElement(new StreamRecord<>(e, e.eventTsUs / 1000));
          }
        } else {
          for (int i = 0; i < 600; i++) {
            h.processElement(new StreamRecord<>(cb.get(i), cb.get(i).eventTsUs / 1000));
            h.processElement(new StreamRecord<>(kr.get(i), kr.get(i).eventTsUs / 1000));
          }
        }
        return h.extractOutputValues();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    List<Spread> a = run.apply(false);
    List<Spread> b = run.apply(true);
    assertTrue(a.size() > 500, "samples " + a.size());
    assertEquals(a.size(), b.size());
    for (int i = 0; i < a.size(); i++) {
      assertEquals(a.get(i).tsMs, b.get(i).tsMs);
      assertEquals(0, a.get(i).spread.compareTo(b.get(i).spread), "tick " + a.get(i).tsMs);
    }
  }
}
