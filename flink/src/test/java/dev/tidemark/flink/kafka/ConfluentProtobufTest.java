package dev.tidemark.flink.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.tidemark.flink.model.TradeEvent;
import dev.tidemark.proto.v1.Side;
import dev.tidemark.proto.v1.Source;
import dev.tidemark.proto.v1.Trade;
import dev.tidemark.proto.v1.Venue;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class ConfluentProtobufTest {

  /** Frame exactly as franz-go's sr.ConfluentHeader does. */
  static byte[] frame(int schemaId, int[] index, byte[] payload) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0);
    out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
    if (index.length == 1 && index[0] == 0) {
      out.write(0);
    } else {
      writeZigZag(out, index.length);
      for (int i : index) {
        writeZigZag(out, i);
      }
    }
    out.write(payload);
    return out.toByteArray();
  }

  static void writeZigZag(ByteArrayOutputStream out, int v) {
    int z = (v << 1) ^ (v >> 31);
    while ((z & ~0x7f) != 0) {
      out.write((z & 0x7f) | 0x80);
      z >>>= 7;
    }
    out.write(z);
  }

  @Test
  void unwrapsFirstMessageShortForm() throws Exception {
    Trade t = Trade.newBuilder().setVenue(Venue.VENUE_KRAKEN).setProductId("BTC-USD").setTradeId(42)
        .setPrice("82184.10000").setSize("0.00058374").setTakerSide(Side.SIDE_BUY)
        .setEventTsNs(1_792_654_390_913_895_000L).setSource(Source.SOURCE_REST).build();
    ConfluentProtobuf.Framed f = ConfluentProtobuf.unwrap(frame(7, new int[] {0}, t.toByteArray()));
    assertEquals(7, f.schemaId());
    assertArrayEquals(new int[] {0}, f.messageIndex());
    TradeEvent e = Deserializers.toEvent(Trade.parseFrom(f.payload()), 3, 99);
    assertEquals("kraken", e.venue);
    assertEquals(42, e.tradeId);
    assertEquals(new BigDecimal("82184.10000"), e.price);
    assertEquals("buy", e.takerSide);
    assertEquals("rest", e.source);
    assertEquals(1_792_654_390_913_895L, e.eventTsUs);
    assertEquals("kraken:BTC-USD:42", e.tradeKey());
  }

  @Test
  void unwrapsLongFormIndex() throws Exception {
    ConfluentProtobuf.Framed f = ConfluentProtobuf.unwrap(frame(300, new int[] {3}, new byte[] {1, 2}));
    assertEquals(300, f.schemaId());
    assertArrayEquals(new int[] {3}, f.messageIndex());
    assertEquals(2, f.payload().remaining());
  }

  @Test
  void rejectsUnframed() {
    assertThrows(IllegalArgumentException.class, () -> ConfluentProtobuf.unwrap(new byte[] {1, 2, 3, 4, 5, 6}));
  }
}
