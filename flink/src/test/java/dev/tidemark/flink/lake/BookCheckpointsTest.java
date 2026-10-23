package dev.tidemark.flink.lake;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.tidemark.flink.model.BookMsg;
import java.math.BigDecimal;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

class BookCheckpointsTest {
  static BookMsg msg(boolean snap, long tsMs, String[][] bids, String[][] asks) {
    BookMsg m = new BookMsg();
    m.venue = "kraken";
    m.productId = "BTC-USD";
    m.isSnapshot = snap;
    m.eventTsUs = tsMs * 1000;
    for (String[] l : bids) {
      m.bids.add(new BookMsg.Level(l[0], l[1]));
    }
    for (String[] l : asks) {
      m.asks.add(new BookMsg.Level(l[0], l[1]));
    }
    return m;
  }

  @Test
  void writesTruncatedBookEveryIntervalOnlyAfterASnapshot() throws Exception {
    try (var h = new KeyedOneInputStreamOperatorTestHarness<String, BookMsg, RowData>(
        new KeyedProcessOperator<>(new BookCheckpoints(10_000, 2)), BookMsg::instrumentKey, Types.STRING)) {
      h.open();
      // Updates before any snapshot: nothing is written at the 10 s tick.
      h.processElement(new StreamRecord<>(msg(false, 1_000, new String[][] {{"50", "1"}}, new String[][] {}), 1_000));
      h.processElement(new StreamRecord<>(msg(true, 12_000,
          new String[][] {{"100", "1"}, {"99", "2"}}, new String[][] {{"101", "3"}}), 12_000));
      assertTrue(h.extractOutputValues().isEmpty());
      h.processElement(new StreamRecord<>(msg(false, 15_000, new String[][] {{"100.5", "4"}}, new String[][] {}), 15_000));
      // The first message past the 20 s tick cuts the checkpoint, before it is applied.
      h.processElement(new StreamRecord<>(msg(false, 21_000, new String[][] {{"100.7", "1"}}, new String[][] {}), 21_000));
      List<RowData> rows = h.extractOutputValues();
      assertEquals(3, rows.size()); // 2 bids (99 truncated away by depth 2) + 1 ask
      assertEquals(20_000_000L, rows.get(0).getTimestamp(2, 6).getMillisecond() * 1000);
      assertEquals("bid", rows.get(0).getString(3).toString());
      assertEquals(1, rows.get(0).getInt(4));
      assertEquals(0, rows.get(0).getDecimal(5, 28, 12).toBigDecimal().compareTo(new BigDecimal("100.5")));
      assertEquals(0, rows.get(1).getDecimal(5, 28, 12).toBigDecimal().compareTo(new BigDecimal("100")));
      assertEquals("ask", rows.get(2).getString(3).toString());
    }
  }

  @Test
  void checkpointNeverContainsLaterMessagesEvenWithoutAWatermark() throws Exception {
    // The bug: timers fired on a lagging watermark after later messages were applied, so the
    // 08:02:30 checkpoint held the 08:02:40 book. With no watermark at all, a checkpoint must
    // still be cut, and it must hold only messages before its tick.
    try (var h = new KeyedOneInputStreamOperatorTestHarness<String, BookMsg, RowData>(
        new KeyedProcessOperator<>(new BookCheckpoints(10_000, 10)), BookMsg::instrumentKey, Types.STRING)) {
      h.open();
      h.processElement(new StreamRecord<>(msg(true, 1_000,
          new String[][] {{"2424.53", "1"}}, new String[][] {{"2424.54", "1"}}), 1_000));
      // A sell-off well after the 10 s tick, then a quiet period that skips the 20 s and 30 s ticks.
      h.processElement(new StreamRecord<>(msg(false, 12_000,
          new String[][] {{"2424.53", "0"}, {"2395.61", "1"}}, new String[][] {{"2424.54", "0"}, {"2395.62", "1"}}),
          12_000));
      h.processElement(new StreamRecord<>(msg(false, 35_000, new String[][] {{"2395.5", "1"}}, new String[][] {}), 35_000));
      List<RowData> rows = h.extractOutputValues();
      assertEquals(4, rows.size()); // checkpoint at 10 s (pre-sell-off) and at 30 s (post), 1 bid + 1 ask each
      assertEquals(0, rows.get(0).getDecimal(5, 28, 12).toBigDecimal().compareTo(new BigDecimal("2424.53")));
      assertEquals(10_000L, rows.get(0).getTimestamp(2, 6).getMillisecond());
      assertEquals(0, rows.get(2).getDecimal(5, 28, 12).toBigDecimal().compareTo(new BigDecimal("2395.61")));
      assertEquals(30_000L, rows.get(2).getTimestamp(2, 6).getMillisecond());
    }
  }
}
