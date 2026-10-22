package dev.tidemark.flink.lake;

import dev.tidemark.flink.model.BookLevelEvent;
import dev.tidemark.flink.model.GapRecord;
import dev.tidemark.flink.model.TradeEvent;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

/** POJO → Iceberg RowData, field order matching the schemas in {@link Lakehouse}. */
public final class Rows {
  static final int PRECISION = 28;
  static final int SCALE = 12;

  private Rows() {}

  static StringData str(String s) {
    return s == null ? null : StringData.fromString(s);
  }

  static DecimalData dec(BigDecimal v) {
    if (v == null) {
      return null;
    }
    return DecimalData.fromBigDecimal(v.setScale(SCALE, RoundingMode.HALF_EVEN), PRECISION, SCALE);
  }

  static TimestampData ts(long micros) {
    if (micros <= 0) {
      return null;
    }
    return TimestampData.fromEpochMillis(Math.floorDiv(micros, 1000), (int) Math.floorMod(micros, 1000) * 1000);
  }

  public static RowData trade(TradeEvent t) {
    return GenericRowData.of(
        str(t.venue), str(t.productId), t.tradeId, dec(t.price), dec(t.size), str(t.takerSide),
        ts(t.eventTsUs), ts(t.recvTsUs), str(t.source), str(t.ingestId), t.fromSnapshot,
        t.kafkaPartition, t.kafkaOffset, t.venueSequence == 0 ? null : t.venueSequence);
  }

  public static RowData book(BookLevelEvent b) {
    return GenericRowData.of(
        str(b.venue), str(b.productId), b.seq, b.isSnapshot, str(b.side), b.levelIndex,
        dec(b.price), dec(b.qty), b.checksum, b.checksumOk, ts(b.eventTsUs), ts(b.recvTsUs),
        str(b.source), b.kafkaPartition, b.kafkaOffset);
  }

  public static RowData gap(GapRecord g) {
    return GenericRowData.of(
        str(g.gapId), str(g.venue), str(g.productId), str(g.kind), str(g.status),
        g.firstMissingTradeId, g.lastMissingTradeId, g.recoveredCount,
        ts(g.detectedTsUs), ts(g.resolvedTsUs), str(g.detail), g.kafkaPartition, g.kafkaOffset);
  }
}
