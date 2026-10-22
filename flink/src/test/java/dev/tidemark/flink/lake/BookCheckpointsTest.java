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
      h.processWatermark(10_000);
      assertTrue(h.extractOutputValues().isEmpty());
      h.processElement(new StreamRecord<>(msg(true, 12_000,
          new String[][] {{"100", "1"}, {"99", "2"}}, new String[][] {{"101", "3"}}), 12_000));
      h.processElement(new StreamRecord<>(msg(false, 15_000, new String[][] {{"100.5", "4"}}, new String[][] {}), 15_000));
      h.processWatermark(20_000);
      List<RowData> rows = h.extractOutputValues();
      assertEquals(3, rows.size()); // 2 bids (99 truncated away by depth 2) + 1 ask
      assertEquals("bid", rows.get(0).getString(3).toString());
      assertEquals(1, rows.get(0).getInt(4));
      assertEquals(0, rows.get(0).getDecimal(5, 28, 12).toBigDecimal().compareTo(new BigDecimal("100.5")));
      assertEquals(0, rows.get(1).getDecimal(5, 28, 12).toBigDecimal().compareTo(new BigDecimal("100")));
      assertEquals("ask", rows.get(2).getString(3).toString());
    }
  }
}
