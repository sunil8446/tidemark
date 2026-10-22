package dev.tidemark.flink.detect;

import dev.tidemark.proto.v1.Alert;
import dev.tidemark.proto.v1.Severity;
import dev.tidemark.proto.v1.Venue;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/** AlertEvent → Confluent-framed Protobuf Alert on alerts.v1, keyed by venue:product. */
public class AlertSerializer implements KafkaRecordSerializationSchema<AlertEvent> {
  /** Message index of Alert within market.proto (see schemas/embed.go). */
  static final int ALERT_INDEX = 4;

  private final String topic;
  private final int schemaId;

  public AlertSerializer(String topic, int schemaId) {
    this.topic = topic;
    this.schemaId = schemaId;
  }

  static Venue venue(String v) {
    return switch (v) {
      case "coinbase" -> Venue.VENUE_COINBASE;
      case "kraken" -> Venue.VENUE_KRAKEN;
      default -> Venue.VENUE_UNSPECIFIED;
    };
  }

  static Severity severity(String s) {
    return switch (s) {
      case "high" -> Severity.SEVERITY_HIGH;
      case "medium" -> Severity.SEVERITY_MEDIUM;
      case "low" -> Severity.SEVERITY_LOW;
      default -> Severity.SEVERITY_UNSPECIFIED;
    };
  }

  public static Alert toProto(AlertEvent a) {
    return Alert.newBuilder()
        .setAlertId(a.alertId)
        .setDetector(a.detector)
        .setVenue(venue(a.venue))
        .setProductId(a.productId)
        .setEventTsNs(a.eventTsMs * 1_000_000)
        .setWindowStartNs(a.windowStartMs * 1_000_000)
        .setWindowEndNs(a.windowEndMs * 1_000_000)
        .setScore(a.score)
        .setThreshold(a.threshold)
        .setSeverity(severity(a.severity))
        .setSummary(a.summary == null ? "" : a.summary)
        .putAllEvidence(a.evidence)
        .setEmittedTsNs(a.emittedTsMs * 1_000_000)
        .build();
  }

  /** Header: magic 0, 4-byte schema id, zig-zag varint index count (1) and index. */
  static byte[] frame(int schemaId, int index, byte[] payload) {
    ByteBuffer b = ByteBuffer.allocate(1 + 4 + 2 + payload.length);
    b.put((byte) 0).putInt(schemaId).put((byte) 2).put((byte) (index << 1)).put(payload);
    return b.array();
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(AlertEvent a, KafkaSinkContext ctx, Long timestamp) {
    String key = (a.venue.isEmpty() ? "x" : a.venue) + ":" + a.productId;
    return new ProducerRecord<>(topic, null, timestamp, key.getBytes(StandardCharsets.UTF_8),
        frame(schemaId, ALERT_INDEX, toProto(a).toByteArray()));
  }
}
