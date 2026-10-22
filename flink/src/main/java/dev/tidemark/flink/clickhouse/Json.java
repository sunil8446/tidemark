package dev.tidemark.flink.clickhouse;

import dev.tidemark.flink.analytics.Bar;
import dev.tidemark.flink.analytics.Spread;
import dev.tidemark.flink.analytics.Vwap;
import dev.tidemark.flink.detect.AlertEvent;
import java.util.Map;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** JSONEachRow renderers for the ClickHouse tables. Decimals are quoted to keep them exact. */
public final class Json {
  private static final DateTimeFormatter TS =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

  private Json() {}

  static String q(String s) {
    return s == null ? "null" : "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  static String d(BigDecimal v) {
    return v == null ? "null" : "\"" + v.toPlainString() + "\"";
  }

  static String ts(long ms) {
    return "\"" + TS.format(Instant.ofEpochMilli(ms)) + "\"";
  }

  public static String bar(Bar b) {
    return "{\"venue\":" + q(b.venue) + ",\"product_id\":" + q(b.productId) + ",\"interval\":" + q(b.interval)
        + ",\"bar_start\":" + ts(b.barStartMs) + ",\"open\":" + d(b.open) + ",\"high\":" + d(b.high)
        + ",\"low\":" + d(b.low) + ",\"close\":" + d(b.close) + ",\"volume\":" + d(b.volume)
        + ",\"notional\":" + d(b.notional) + ",\"vwap\":" + d(b.vwap) + ",\"buy_volume\":" + d(b.buyVolume)
        + ",\"sell_volume\":" + d(b.sellVolume) + ",\"trade_count\":" + b.tradeCount
        + ",\"revision\":" + b.revision + "}";
  }

  public static String vwap(Vwap v) {
    return "{\"venue\":" + q(v.venue) + ",\"product_id\":" + q(v.productId) + ",\"window\":" + q(v.window)
        + ",\"window_end\":" + ts(v.windowEndMs) + ",\"vwap\":" + d(v.vwap) + ",\"volume\":" + d(v.volume)
        + ",\"trade_count\":" + v.tradeCount + "}";
  }

  public static String alert(AlertEvent a, String stream) {
    StringBuilder ev = new StringBuilder("{");
    for (Map.Entry<String, String> e : a.evidence.entrySet()) {
      if (ev.length() > 1) {
        ev.append(',');
      }
      ev.append(q(e.getKey())).append(':').append(q(e.getValue()));
    }
    ev.append('}');
    return "{\"stream\":" + q(stream) + ",\"alert_id\":" + q(a.alertId) + ",\"detector\":" + q(a.detector)
        + ",\"venue\":" + q(a.venue) + ",\"product_id\":" + q(a.productId) + ",\"event_ts\":" + ts(a.eventTsMs)
        + ",\"window_start\":" + ts(a.windowStartMs) + ",\"window_end\":" + ts(a.windowEndMs)
        + ",\"score\":" + a.score + ",\"threshold\":" + a.threshold + ",\"severity\":" + q(a.severity)
        + ",\"summary\":" + q(a.summary) + ",\"evidence\":" + ev + ",\"emitted_at\":" + ts(a.emittedTsMs) + "}";
  }

  public static String spread(Spread s) {
    return "{\"product_id\":" + q(s.productId) + ",\"ts\":" + ts(s.tsMs) + ",\"coinbase_price\":" + d(s.coinbasePrice)
        + ",\"kraken_price\":" + d(s.krakenPrice) + ",\"coinbase_age_ms\":" + s.coinbaseAgeMs
        + ",\"kraken_age_ms\":" + s.krakenAgeMs + ",\"spread\":" + d(s.spread)
        + ",\"spread_bps\":" + s.spreadBps + "}";
  }
}
