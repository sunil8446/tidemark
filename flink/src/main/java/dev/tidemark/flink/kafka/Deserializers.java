package dev.tidemark.flink.kafka;

import dev.tidemark.flink.model.BookLevelEvent;
import dev.tidemark.flink.model.GapRecord;
import dev.tidemark.flink.model.TradeEvent;
import dev.tidemark.proto.v1.BookLevel;
import dev.tidemark.proto.v1.BookUpdate;
import dev.tidemark.proto.v1.GapEvent;
import dev.tidemark.proto.v1.Trade;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Kafka deserializers from Confluent-framed Protobuf to Flink POJOs. */
public final class Deserializers {
  private Deserializers() {}

  static String enumName(Enum<?> e, String prefix) {
    return e.name().startsWith(prefix) ? e.name().substring(prefix.length()).toLowerCase() : e.name().toLowerCase();
  }

  static BigDecimal decimal(String s) {
    return s == null || s.isEmpty() ? null : new BigDecimal(s);
  }

  public static TradeEvent toEvent(Trade t, int partition, long offset) {
    TradeEvent e = new TradeEvent();
    e.venue = enumName(t.getVenue(), "VENUE_");
    e.productId = t.getProductId();
    e.tradeId = t.getTradeId();
    e.price = decimal(t.getPrice());
    e.size = decimal(t.getSize());
    e.takerSide = enumName(t.getTakerSide(), "SIDE_");
    e.eventTsUs = t.getEventTsNs() / 1_000;
    e.recvTsUs = t.getRecvTsNs() / 1_000;
    e.source = enumName(t.getSource(), "SOURCE_");
    e.ingestId = t.getIngestId();
    e.fromSnapshot = t.getFromSnapshot();
    e.venueSequence = t.getVenueSequence();
    e.kafkaPartition = partition;
    e.kafkaOffset = offset;
    return e;
  }

  /** trades.v1 → TradeEvent. Malformed records are skipped and counted by the caller's metrics. */
  public static class Trades implements KafkaRecordDeserializationSchema<TradeEvent> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<TradeEvent> out) throws IOException {
      ConfluentProtobuf.Framed f = ConfluentProtobuf.unwrap(r.value());
      out.collect(toEvent(Trade.parseFrom(f.payload()), r.partition(), r.offset()));
    }

    @Override
    public TypeInformation<TradeEvent> getProducedType() {
      return TypeInformation.of(TradeEvent.class);
    }
  }

  /** book.v1 → one BookLevelEvent per level. */
  public static class Books implements KafkaRecordDeserializationSchema<BookLevelEvent> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<BookLevelEvent> out) throws IOException {
      ConfluentProtobuf.Framed f = ConfluentProtobuf.unwrap(r.value());
      BookUpdate b = BookUpdate.parseFrom(f.payload());
      emit(b, "bid", b.getBidsList(), r, out);
      emit(b, "ask", b.getAsksList(), r, out);
    }

    private static void emit(BookUpdate b, String side, List<BookLevel> levels,
        ConsumerRecord<byte[], byte[]> r, Collector<BookLevelEvent> out) {
      for (int i = 0; i < levels.size(); i++) {
        BookLevelEvent e = new BookLevelEvent();
        e.venue = enumName(b.getVenue(), "VENUE_");
        e.productId = b.getProductId();
        e.seq = b.getSeq();
        e.isSnapshot = b.getIsSnapshot();
        e.side = side;
        e.levelIndex = i;
        e.price = decimal(levels.get(i).getPrice());
        e.qty = decimal(levels.get(i).getQty());
        e.checksum = Integer.toUnsignedLong(b.getChecksum());
        e.checksumOk = b.getChecksumOk();
        e.eventTsUs = b.getEventTsNs() / 1_000;
        e.recvTsUs = b.getRecvTsNs() / 1_000;
        e.source = enumName(b.getSource(), "SOURCE_");
        e.kafkaPartition = r.partition();
        e.kafkaOffset = r.offset();
        out.collect(e);
      }
    }

    @Override
    public TypeInformation<BookLevelEvent> getProducedType() {
      return TypeInformation.of(BookLevelEvent.class);
    }
  }

  /** gaps.v1 → GapRecord. */
  public static class Gaps implements KafkaRecordDeserializationSchema<GapRecord> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<GapRecord> out) throws IOException {
      ConfluentProtobuf.Framed f = ConfluentProtobuf.unwrap(r.value());
      GapEvent g = GapEvent.parseFrom(f.payload());
      GapRecord e = new GapRecord();
      e.gapId = g.getGapId();
      e.venue = enumName(g.getVenue(), "VENUE_");
      e.productId = g.getProductId();
      e.kind = enumName(g.getKind(), "GAP_KIND_");
      e.status = enumName(g.getStatus(), "GAP_STATUS_");
      e.firstMissingTradeId = g.getFirstMissingTradeId();
      e.lastMissingTradeId = g.getLastMissingTradeId();
      e.recoveredCount = g.getRecoveredCount();
      e.detectedTsUs = g.getDetectedTsNs() / 1_000;
      e.resolvedTsUs = g.getResolvedTsNs() / 1_000;
      e.detail = g.getDetail();
      e.kafkaPartition = r.partition();
      e.kafkaOffset = r.offset();
      out.collect(e);
    }

    @Override
    public TypeInformation<GapRecord> getProducedType() {
      return TypeInformation.of(GapRecord.class);
    }
  }
}
