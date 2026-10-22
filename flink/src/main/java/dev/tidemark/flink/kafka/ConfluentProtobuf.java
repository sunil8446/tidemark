package dev.tidemark.flink.kafka;

import java.nio.ByteBuffer;

/**
 * Strips the Confluent schema-registry wire header from a record value:
 * magic byte 0, a 4-byte big-endian schema id, then the Protobuf message-index
 * path as zig-zag varints (a single 0 byte stands for the path [0]).
 */
public final class ConfluentProtobuf {
  private ConfluentProtobuf() {}

  /** The payload after the header, plus the schema id and message index path. */
  public record Framed(int schemaId, int[] messageIndex, ByteBuffer payload) {}

  public static Framed unwrap(byte[] value) {
    if (value == null || value.length < 6 || value[0] != 0) {
      throw new IllegalArgumentException("not a Confluent-framed record");
    }
    ByteBuffer buf = ByteBuffer.wrap(value);
    buf.get(); // magic
    int schemaId = buf.getInt();
    int count = readZigZag(buf);
    int[] index;
    if (count == 0) {
      index = new int[] {0};
    } else {
      index = new int[count];
      for (int i = 0; i < count; i++) {
        index[i] = readZigZag(buf);
      }
    }
    return new Framed(schemaId, index, buf.slice());
  }

  private static int readZigZag(ByteBuffer buf) {
    int result = 0;
    int shift = 0;
    while (true) {
      if (!buf.hasRemaining()) {
        throw new IllegalArgumentException("truncated varint in header");
      }
      byte b = buf.get();
      result |= (b & 0x7f) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
      if (shift > 28) {
        throw new IllegalArgumentException("varint too long in header");
      }
    }
    return (result >>> 1) ^ -(result & 1);
  }
}
