package dev.tidemark.flink.clickhouse;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.Counter;

/**
 * Batches rows as JSONEachRow and POSTs them to ClickHouse over HTTP.
 * Flushes when the batch is full, every {@code flushMs}, and on every
 * checkpoint. That gives at-least-once delivery; target tables are
 * ReplacingMergeTree keyed on the natural key, so a replayed row overwrites
 * itself instead of duplicating.
 */
public class ClickHouseSink<T> implements Sink<T> {

  /** Renders one row as a JSON object. */
  public interface RowFormatter<T> extends Serializable {
    String toJson(T row);
  }

  private final String url;
  private final String user;
  private final String password;
  private final String table;
  private final RowFormatter<T> formatter;
  private final int maxRows;
  private final long flushMs;

  public ClickHouseSink(String url, String user, String password, String table, RowFormatter<T> formatter,
      int maxRows, long flushMs) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.table = table;
    this.formatter = formatter;
    this.maxRows = maxRows;
    this.flushMs = flushMs;
  }

  @Override
  @Deprecated
  public SinkWriter<T> createWriter(InitContext context) {
    return new Writer(null, null, null);
  }

  @Override
  public SinkWriter<T> createWriter(WriterInitContext context) {
    return new Writer(context.getProcessingTimeService(),
        context.metricGroup().counter("tidemark_clickhouse_rows"),
        context.metricGroup().counter("tidemark_clickhouse_batches"));
  }

  private class Writer implements SinkWriter<T> {
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final StringBuilder batch = new StringBuilder();
    private final ProcessingTimeService timers;
    private final Counter rows;
    private final Counter batches;
    private final URI insertUri;
    private int pending;

    Writer(ProcessingTimeService timers, Counter rows, Counter batches) {
      this.timers = timers;
      this.rows = rows;
      this.batches = batches;
      String q = "INSERT INTO " + table + " FORMAT JSONEachRow";
      this.insertUri = URI.create(url + "/?query=" + URLEncoder.encode(q, StandardCharsets.UTF_8)
          + "&date_time_input_format=best_effort");
      schedule();
    }

    private void schedule() {
      if (timers != null) {
        timers.registerTimer(timers.getCurrentProcessingTime() + flushMs, ts -> {
          send();
          schedule();
        });
      }
    }

    @Override
    public void write(T element, Context context) throws IOException {
      batch.append(formatter.toJson(element)).append('\n');
      pending++;
      if (pending >= maxRows) {
        send();
      }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
      send();
    }

    private void send() throws IOException {
      if (pending == 0) {
        return;
      }
      HttpRequest req = HttpRequest.newBuilder(insertUri)
          .timeout(Duration.ofSeconds(30))
          .header("X-ClickHouse-User", user)
          .header("X-ClickHouse-Key", password)
          .POST(HttpRequest.BodyPublishers.ofString(batch.toString()))
          .build();
      IOException last = null;
      for (int attempt = 0; attempt < 5; attempt++) {
        try {
          HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
          if (resp.statusCode() == 200) {
            if (rows != null) {
              rows.inc(pending);
              batches.inc();
            }
            batch.setLength(0);
            pending = 0;
            return;
          }
          last = new IOException("clickhouse " + table + ": HTTP " + resp.statusCode() + " " + resp.body());
          if (resp.statusCode() >= 400 && resp.statusCode() < 500) {
            break; // bad data; retrying will not help
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        } catch (IOException e) {
          last = e;
        }
        try {
          Thread.sleep(200L << attempt);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
      }
      throw last; // fails the task; Flink restarts from the last checkpoint
    }

    @Override
    public void close() throws Exception {
      send();
    }
  }
}
