package dev.tidemark.flink.detect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.tidemark.flink.analytics.Spread;
import dev.tidemark.flink.model.BookMsg;
import dev.tidemark.flink.model.TradeEvent;
import java.math.BigDecimal;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class DetectorsTest {

  static TradeEvent trade(String venue, long id, String price, String size, long tsMs) {
    TradeEvent t = new TradeEvent();
    t.venue = venue;
    t.productId = "BTC-USD";
    t.tradeId = id;
    t.price = new BigDecimal(price);
    t.size = new BigDecimal(size);
    t.eventTsUs = tsMs * 1000;
    t.takerSide = "buy";
    return t;
  }

  // ------------------------------------------------------------ volume spike

  static KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, AlertEvent> spike() throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(new VolumeSpike(10, 600, 30, 6.0, 5.0, 50_000, 5.0)),
        TradeEvent::instrumentKey, Types.STRING);
  }

  @Test
  void spikeFiresOnlyAfterWarmBaselineAndForLargeBuckets() throws Exception {
    try (var h = spike()) {
      h.open();
      long id = 1;
      // 40 buckets of steady ~10k notional with small jitter.
      for (int b = 0; b < 40; b++) {
        long ts = b * 10_000L + 500;
        String size = b % 2 == 0 ? "0.10" : "0.12";
        h.processElement(new StreamRecord<>(trade("kraken", id++, "100000", size, ts), ts));
        h.processWatermark(ts + 10_000);
      }
      assertTrue(h.extractOutputValues().isEmpty(), "steady volume must not alert");
      // One bucket at 50x: 550k notional.
      long ts = 40 * 10_000L + 500;
      h.processElement(new StreamRecord<>(trade("kraken", id++, "100000", "5.5", ts), ts));
      h.processWatermark(ts + 10_000);
      List<AlertEvent> out = h.extractOutputValues();
      assertTrue(!out.isEmpty());
      AlertEvent a = out.get(out.size() - 1);
      for (AlertEvent e : out) {
        assertEquals(a.alertId, e.alertId, "an escalating episode keeps one alert id");
      }
      assertEquals("volume_spike", a.detector);
      assertEquals("kraken", a.venue);
      assertTrue(a.windowStartMs <= 400_500 && a.windowEndMs > 400_500, "window covers the burst");
      assertTrue(a.score > 6, "score " + a.score);
      assertEquals("0.0", a.evidence.get("price_range_bps"), "single-price burst has no price impact");
    }
  }

  /** Peak score of a 550k burst after a steady baseline; the burst either prints at one price or sweeps 100 bps. */
  static double burstScore(boolean movesPrice) throws Exception {
    try (var h = spike()) {
      h.open();
      long id = 1;
      for (int b = 0; b < 40; b++) {
        long ts = b * 10_000L + 500;
        h.processElement(new StreamRecord<>(trade("kraken", id++, "100000", b % 2 == 0 ? "0.10" : "0.12", ts), ts));
        h.processWatermark(ts + 10_000);
      }
      long ts = 400_500;
      h.processElement(new StreamRecord<>(trade("kraken", id++, "100000", "2.75", ts), ts));
      h.processElement(new StreamRecord<>(trade("kraken", id++, movesPrice ? "101000" : "100000", "2.72", ts + 300), ts + 300));
      h.processWatermark(ts + 10_000);
      return h.extractOutputValues().stream().mapToDouble(x -> x.score).max().orElse(0);
    }
  }

  @Test
  void spikeThatMovesPriceScoresLower() throws Exception {
    double flat = burstScore(false);
    double sweep = burstScore(true);
    // 100 bps of impact with impactBps=5 divides the score by 21.
    assertTrue(flat > 6 && sweep < flat / 15, "flat " + flat + " sweep " + sweep);
  }

  @Test
  void spikeIgnoresQuietProductBelowNotionalFloor() throws Exception {
    try (var h = spike()) {
      h.open();
      // Baseline of empty buckets, then one 1k-notional trade: infinite z, but under the floor.
      h.processElement(new StreamRecord<>(trade("kraken", 1, "100", "1", 500), 500));
      h.processWatermark(10_000);
      h.processElement(new StreamRecord<>(trade("kraken", 2, "100", "10", 400_500), 400_500));
      h.processWatermark(410_000);
      assertTrue(h.extractOutputValues().isEmpty());
    }
  }

  // ------------------------------------------------------------ spoof layering

  static BookMsg book(boolean snapshot, long tsMs, String[][] bids, String[][] asks) {
    BookMsg m = new BookMsg();
    m.venue = "kraken";
    m.productId = "BTC-USD";
    m.isSnapshot = snapshot;
    m.eventTsUs = tsMs * 1000;
    for (String[] l : bids) {
      m.bids.add(new BookMsg.Level(l[0], l[1]));
    }
    for (String[] l : asks) {
      m.asks.add(new BookMsg.Level(l[0], l[1]));
    }
    return m;
  }

  static KeyedTwoInputStreamOperatorTestHarness<String, BookMsg, TradeEvent, AlertEvent> spoof() throws Exception {
    var h = new KeyedTwoInputStreamOperatorTestHarness<>(
        new KeyedCoProcessOperator<>(new SpoofLayering(3, 1.0, 10_000, 0.8, 0.1, 250, 1_000, 10)),
        BookMsg::instrumentKey, TradeEvent::instrumentKey, Types.STRING);
    h.open();
    String[][] bids = {{"100.0", "1"}, {"99.9", "1"}, {"99.8", "1"}, {"99.7", "1"}, {"99.6", "1"}};
    String[][] asks = {{"100.1", "1"}, {"100.2", "1"}, {"100.3", "1"}, {"100.4", "1"}, {"100.5", "1"}};
    h.processElement1(new StreamRecord<>(book(true, 1_000, bids, asks), 1_000));
    return h;
  }

  @Test
  void spoofFiresWhenLargeAddIsPulledWithoutTrades() throws Exception {
    try (var h = spoof()) {
      // 20 BTC added at the second bid level (median level is 1), pulled 3 s later.
      h.processElement1(new StreamRecord<>(book(false, 2_000, new String[][] {{"99.90", "21"}}, new String[][] {}), 2_000));
      h.processElement1(new StreamRecord<>(book(false, 5_000, new String[][] {{"99.9", "1"}}, new String[][] {}), 5_000));
      assertTrue(h.extractOutputValues().isEmpty(), "a pull is only confirmed after the repricing window");
      h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(7_000));
      List<AlertEvent> out = h.extractOutputValues();
      assertEquals(1, out.size());
      AlertEvent a = out.get(0);
      assertEquals("spoof_layering", a.detector);
      assertEquals(5.0, a.score, 1e-9); // 20 added vs 4 resting on the other bid levels
      assertEquals("bid", a.evidence.get("side"));
      assertEquals("2", a.evidence.get("rank"));
      assertEquals("3000", a.evidence.get("lifetime_ms"));
    }
  }

  @Test
  void spoofIgnoresAddThatTradesOrStays() throws Exception {
    try (var h = spoof()) {
      // Large add that gets hit by trades (genuine liquidity), then disappears.
      h.processElement1(new StreamRecord<>(book(false, 2_000, new String[][] {{"100.0", "21"}}, new String[][] {}), 2_000));
      h.processElement2(new StreamRecord<>(trade("kraken", 1, "100.0", "15", 2_500), 2_500));
      h.processElement1(new StreamRecord<>(book(false, 3_000, new String[][] {{"100.0", "0"}}, new String[][] {}), 3_000));
      // Large add that stays past the max lifetime.
      h.processElement1(new StreamRecord<>(book(false, 4_000, new String[][] {}, new String[][] {{"100.2", "30"}}), 4_000));
      h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(20_000));
      h.processElement1(new StreamRecord<>(book(false, 21_000, new String[][] {}, new String[][] {{"100.2", "1"}}), 21_000));
      // Large add deep in the book (rank 5) is out of scope.
      h.processElement1(new StreamRecord<>(book(false, 22_000, new String[][] {{"99.6", "40"}}, new String[][] {}), 22_000));
      h.processElement1(new StreamRecord<>(book(false, 23_000, new String[][] {{"99.6", "1"}}, new String[][] {}), 23_000));
      assertTrue(h.extractOutputValues().isEmpty(), "got " + h.extractOutputValues().size());
    }
  }

  @Test
  void tradesAgainstTheQueueAheadDoNotCountAsFills() throws Exception {
    try (var h = spoof()) {
      // Best bid 100.0 had 1 resting; 20 added behind it. 0.9 traded there only eats the order ahead.
      h.processElement1(new StreamRecord<>(book(false, 2_000, new String[][] {{"100.0", "21"}}, new String[][] {}), 2_000));
      h.processElement2(new StreamRecord<>(trade("kraken", 1, "100.0", "0.9", 2_500), 2_500));
      h.processElement1(new StreamRecord<>(book(false, 4_000, new String[][] {{"100.0", "0.1"}}, new String[][] {}), 4_000));
      h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(6_000));
      assertEquals(1, h.extractOutputValues().size());
      assertEquals("0.0", h.extractOutputValues().get(0).evidence.get("filled_estimate"));
    }
  }

  @Test
  void levelsOutsideTheSubscribedDepthAreDropped() throws Exception {
    try (var h = new KeyedTwoInputStreamOperatorTestHarness<>(
        new KeyedCoProcessOperator<>(new SpoofLayering(1, 1.0, 10_000, 0.8, 0.1, 250, 1_000, 2)),
        BookMsg::instrumentKey, TradeEvent::instrumentKey, Types.STRING)) {
      h.open();
      h.processElement1(new StreamRecord<>(book(true, 1_000, new String[][] {{"100", "1"}, {"99", "1"}}, new String[][] {}), 1_000));
      // Market rises: 101 and 102 push 100 and 99 out of a depth-2 book (Kraken sends no delete).
      h.processElement1(new StreamRecord<>(book(false, 2_000, new String[][] {{"101", "1"}, {"102", "1"}}, new String[][] {}), 2_000));
      // Market falls back: 102 and 101 are deleted. A phantom 100 must not reappear as best.
      h.processElement1(new StreamRecord<>(book(false, 3_000,
          new String[][] {{"102", "0"}, {"101", "0"}, {"98", "1"}, {"97", "1"}}, new String[][] {}), 3_000));
      // A large add at 98 is now the best level (rank 1), so it is a candidate; pulled 2 s later.
      h.processElement1(new StreamRecord<>(book(false, 4_000, new String[][] {{"98", "30"}}, new String[][] {}), 4_000));
      h.processElement1(new StreamRecord<>(book(false, 6_000, new String[][] {{"98", "1"}}, new String[][] {}), 6_000));
      h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(8_000));
      assertEquals(1, h.extractOutputValues().size(), "98 must rank first once phantom levels are gone");
      assertEquals("1", h.extractOutputValues().get(0).evidence.get("rank"));
    }
  }

  @Test
  void spoofIgnoresRepricingAndFlicker() throws Exception {
    try (var h = spoof()) {
      // Amend: 20 pulled from 99.9 and re-added at 99.8 within the repricing window.
      h.processElement1(new StreamRecord<>(book(false, 2_000, new String[][] {{"99.9", "21"}}, new String[][] {}), 2_000));
      h.processElement1(new StreamRecord<>(book(false, 3_000, new String[][] {{"99.9", "1"}}, new String[][] {}), 3_000));
      h.processElement1(new StreamRecord<>(book(false, 3_100, new String[][] {{"99.8", "21"}}, new String[][] {}), 3_100));
      // Flicker: added and pulled within 100 ms.
      h.processElement1(new StreamRecord<>(book(false, 8_000, new String[][] {}, new String[][] {{"100.1", "25"}}), 8_000));
      h.processElement1(new StreamRecord<>(book(false, 8_100, new String[][] {}, new String[][] {{"100.1", "1"}}), 8_100));
      h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(30_000));
      assertTrue(h.extractOutputValues().isEmpty(), "got " + h.extractOutputValues().size());
    }
  }

  // ------------------------------------------------------------ dislocation

  static Spread spread(long tsMs, double bps) {
    Spread s = new Spread();
    s.productId = "BTC-USD";
    s.tsMs = tsMs;
    s.spreadBps = bps;
    s.coinbasePrice = new BigDecimal("100");
    s.krakenPrice = new BigDecimal("100");
    return s;
  }

  @Test
  void dislocationNeedsASustainedRun() throws Exception {
    try (var h = new KeyedOneInputStreamOperatorTestHarness<String, Spread, AlertEvent>(
        new KeyedProcessOperator<>(new Dislocation(600, 300, 5.0, 5.0, 10, 5_000)), s -> s.productId, Types.STRING)) {
      h.open();
      long ts = 0;
      for (int i = 0; i < 400; i++, ts += 1000) {
        h.processElement(new StreamRecord<>(spread(ts, (i % 5) * 0.2 - 0.4), ts)); // noise within ±0.4 bps
      }
      for (int i = 0; i < 5; i++, ts += 1000) { // short blip: 5 s at 20 bps
        h.processElement(new StreamRecord<>(spread(ts, 20), ts));
      }
      h.processElement(new StreamRecord<>(spread(ts, 0), ts));
      ts += 1000;
      assertTrue(h.extractOutputValues().isEmpty(), "a 5 s blip must not fire with sustain=10");
      for (int i = 0; i < 15; i++, ts += 1000) { // sustained: 15 s at 25 bps
        h.processElement(new StreamRecord<>(spread(ts, 25), ts));
      }
      List<AlertEvent> out = h.extractOutputValues();
      assertTrue(!out.isEmpty());
      for (AlertEvent e : out) {
        assertEquals(out.get(0).alertId, e.alertId, "one alert id per episode");
      }
      assertEquals("dislocation", out.get(0).detector);
      assertEquals("", out.get(0).venue);
      // A stale venue price is not evidence.
      Spread stale = spread(ts, 30);
      stale.krakenAgeMs = 20_000;
      int before = h.extractOutputValues().size();
      h.processElement(new StreamRecord<>(stale, ts));
      assertEquals(before, h.extractOutputValues().size());
    }
  }

  // ------------------------------------------------------------ wash trading

  static TradeEvent acct(long id, String buyer, String seller, String size, String price, long tsMs) {
    TradeEvent t = trade("coinbase", id, price, size, tsMs);
    t.buyerAccount = buyer;
    t.sellerAccount = seller;
    return t;
  }

  @Test
  void washDetectsSelfTradesAndRoundTrips() throws Exception {
    try (var h = new KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, AlertEvent>(
        new KeyedProcessOperator<>(new WashTrade(60_000, 0.95)), TradeEvent::instrumentKey, Types.STRING)) {
      h.open();
      h.processElement(new StreamRecord<>(acct(1, "A", "A", "1.0", "100", 1_000), 1_000)); // self trade
      h.processElement(new StreamRecord<>(acct(2, "B", "C", "2.0", "100", 2_000), 2_000)); // C sells to B
      h.processElement(new StreamRecord<>(acct(3, "D", "E", "1.0", "100", 3_000), 3_000)); // unrelated
      h.processElement(new StreamRecord<>(acct(4, "C", "B", "2.0", "100.01", 10_000), 10_000)); // B sells back to C
      h.processElement(new StreamRecord<>(acct(5, "E", "D", "3.0", "100", 11_000), 11_000)); // size differs 3x
      List<AlertEvent> out = h.extractOutputValues();
      assertEquals(2, out.size());
      assertEquals("self_trade", out.get(0).evidence.get("kind"));
      assertEquals("round_trip", out.get(1).evidence.get("kind"));
      assertTrue(out.get(1).score > 0.99);
    }
  }

  @Test
  void washIgnoresTradesWithoutAccounts() throws Exception {
    try (var h = new KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, AlertEvent>(
        new KeyedProcessOperator<>(new WashTrade(60_000, 0.95)), TradeEvent::instrumentKey, Types.STRING)) {
      h.open();
      h.processElement(new StreamRecord<>(trade("coinbase", 1, "100", "1", 1_000), 1_000));
      assertTrue(h.extractOutputValues().isEmpty());
    }
  }

  @Test
  void alertFrameMatchesConfluentLongForm() {
    byte[] f = AlertSerializer.frame(7, 4, new byte[] {9});
    assertEquals(0, f[0]);
    assertEquals(7, java.nio.ByteBuffer.wrap(f, 1, 4).getInt());
    assertEquals(2, f[5]); // zig-zag(1): one index
    assertEquals(8, f[6]); // zig-zag(4)
    assertEquals(9, f[7]);
  }
}
