package dev.tidemark.flink.dedup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.tidemark.flink.model.TradeEvent;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class DedupTradesTest {

  static TradeEvent trade(String venue, long id) {
    TradeEvent t = new TradeEvent();
    t.venue = venue;
    t.productId = "BTC-USD";
    t.tradeId = id;
    return t;
  }

  static KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, TradeEvent> harness() throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(new DedupTrades(Duration.ofHours(1))), TradeEvent::tradeKey, Types.STRING);
  }

  static List<Long> ids(KeyedOneInputStreamOperatorTestHarness<String, TradeEvent, TradeEvent> h) {
    return h.extractOutputValues().stream().map(t -> t.tradeId).collect(Collectors.toList());
  }

  @Test
  void dropsRepeatsPerVenueProductAndId() throws Exception {
    try (var h = harness()) {
      h.open();
      long ts = 0;
      for (long id : new long[] {1, 2, 2, 3, 1, 3, 4}) {
        h.processElement(new StreamRecord<>(trade("coinbase", id), ts++));
      }
      h.processElement(new StreamRecord<>(trade("kraken", 1), ts++)); // other venue, same id
      assertEquals(List.of(1L, 2L, 3L, 4L, 1L), ids(h));
    }
  }

  @Test
  void dedupStateSurvivesRestoreFromCheckpoint() throws Exception {
    OperatorSubtaskState snapshot;
    try (var h = harness()) {
      h.open();
      h.processElement(new StreamRecord<>(trade("coinbase", 10), 0));
      h.processElement(new StreamRecord<>(trade("coinbase", 11), 1));
      snapshot = h.snapshot(1L, 2L);
    }
    try (var restored = harness()) {
      restored.initializeState(snapshot);
      restored.open();
      // Replayed after a failure: 10 and 11 were emitted before the checkpoint.
      restored.processElement(new StreamRecord<>(trade("coinbase", 10), 0));
      restored.processElement(new StreamRecord<>(trade("coinbase", 11), 1));
      restored.processElement(new StreamRecord<>(trade("coinbase", 12), 2));
      assertEquals(List.of(12L), ids(restored));
    }
  }
}
