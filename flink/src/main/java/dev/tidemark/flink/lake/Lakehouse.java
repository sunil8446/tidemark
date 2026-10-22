package dev.tidemark.flink.lake;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.types.Types;

/**
 * Iceberg catalog wiring and table definitions. The JDBC catalog lives in
 * Postgres (shared with Trino), data files in MinIO/S3.
 */
public final class Lakehouse {
  public static final String NAMESPACE = "market";

  private Lakehouse() {}

  public static CatalogLoader catalogLoader(ParameterTool p) {
    Map<String, String> props = new HashMap<>();
    props.put("uri", p.get("catalog.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog"));
    props.put("jdbc.user", p.get("catalog.user", "tidemark"));
    props.put("jdbc.password", p.get("catalog.password", "tidemark-local-dev"));
    props.put("jdbc.schema-version", "V1");
    props.put("warehouse", p.get("warehouse", "s3://lake/warehouse"));
    props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    props.put("s3.endpoint", p.get("s3.endpoint", "http://minio:9000"));
    props.put("s3.path-style-access", "true");
    props.put("s3.access-key-id", p.get("s3.access-key", "tidemark"));
    props.put("s3.secret-access-key", p.get("s3.secret-key", "tidemark-local-dev"));
    props.put("client.region", p.get("s3.region", "us-east-1"));
    // Must match iceberg.jdbc-catalog.catalog-name in Trino.
    return CatalogLoader.custom("tidemark", props, new Configuration(), "org.apache.iceberg.jdbc.JdbcCatalog");
  }

  public static final Schema RAW_TRADES = new Schema(
      Types.NestedField.required(1, "venue", Types.StringType.get()),
      Types.NestedField.required(2, "product_id", Types.StringType.get()),
      Types.NestedField.required(3, "trade_id", Types.LongType.get()),
      Types.NestedField.optional(4, "price", Types.DecimalType.of(28, 12)),
      Types.NestedField.optional(5, "size", Types.DecimalType.of(28, 12)),
      Types.NestedField.optional(6, "taker_side", Types.StringType.get()),
      Types.NestedField.required(7, "event_ts", Types.TimestampType.withZone()),
      Types.NestedField.optional(8, "recv_ts", Types.TimestampType.withZone()),
      Types.NestedField.optional(9, "source", Types.StringType.get()),
      Types.NestedField.optional(10, "ingest_id", Types.StringType.get()),
      Types.NestedField.optional(11, "from_snapshot", Types.BooleanType.get()),
      Types.NestedField.optional(12, "kafka_partition", Types.IntegerType.get()),
      Types.NestedField.optional(13, "kafka_offset", Types.LongType.get()));

  public static final Schema BOOK_DELTAS = new Schema(
      Types.NestedField.required(1, "venue", Types.StringType.get()),
      Types.NestedField.required(2, "product_id", Types.StringType.get()),
      Types.NestedField.required(3, "seq", Types.LongType.get()),
      Types.NestedField.optional(4, "is_snapshot", Types.BooleanType.get()),
      Types.NestedField.required(5, "side", Types.StringType.get()),
      Types.NestedField.optional(6, "level_index", Types.IntegerType.get()),
      Types.NestedField.optional(7, "price", Types.DecimalType.of(28, 12)),
      Types.NestedField.optional(8, "qty", Types.DecimalType.of(28, 12)),
      Types.NestedField.optional(9, "checksum", Types.LongType.get()),
      Types.NestedField.optional(10, "checksum_ok", Types.BooleanType.get()),
      Types.NestedField.required(11, "event_ts", Types.TimestampType.withZone()),
      Types.NestedField.optional(12, "recv_ts", Types.TimestampType.withZone()),
      Types.NestedField.optional(13, "source", Types.StringType.get()),
      Types.NestedField.optional(14, "kafka_partition", Types.IntegerType.get()),
      Types.NestedField.optional(15, "kafka_offset", Types.LongType.get()));

  public static final Schema GAP_EVENTS = new Schema(
      Types.NestedField.required(1, "gap_id", Types.StringType.get()),
      Types.NestedField.required(2, "venue", Types.StringType.get()),
      Types.NestedField.required(3, "product_id", Types.StringType.get()),
      Types.NestedField.optional(4, "kind", Types.StringType.get()),
      Types.NestedField.required(5, "status", Types.StringType.get()),
      Types.NestedField.optional(6, "first_missing_trade_id", Types.LongType.get()),
      Types.NestedField.optional(7, "last_missing_trade_id", Types.LongType.get()),
      Types.NestedField.optional(8, "recovered_count", Types.LongType.get()),
      Types.NestedField.required(9, "detected_ts", Types.TimestampType.withZone()),
      Types.NestedField.optional(10, "resolved_ts", Types.TimestampType.withZone()),
      Types.NestedField.optional(11, "detail", Types.StringType.get()),
      Types.NestedField.optional(12, "kafka_partition", Types.IntegerType.get()),
      Types.NestedField.optional(13, "kafka_offset", Types.LongType.get()));

  /** Hidden partitioning: queries filter on event_ts and venue; nobody writes a partition column. */
  static PartitionSpec byDayAndVenue(Schema s, String tsColumn) {
    return PartitionSpec.builderFor(s).day(tsColumn).identity("venue").build();
  }

  public static TableLoader ensureTable(CatalogLoader loader, String name, Schema schema, String tsColumn) {
    Catalog catalog = loader.loadCatalog();
    Namespace ns = Namespace.of(NAMESPACE);
    if (catalog instanceof SupportsNamespaces sn && !sn.namespaceExists(ns)) {
      try {
        sn.createNamespace(ns);
      } catch (AlreadyExistsException ignored) {
        // created concurrently
      }
    }
    TableIdentifier id = TableIdentifier.of(ns, name);
    if (!catalog.tableExists(id)) {
      try {
        Table t = catalog.createTable(id, schema, byDayAndVenue(schema, tsColumn), Map.of(
            TableProperties.FORMAT_VERSION, "2",
            TableProperties.DEFAULT_FILE_FORMAT, "parquet",
            TableProperties.PARQUET_COMPRESSION, "zstd",
            TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true",
            TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "50"));
        System.out.println("created iceberg table " + t.name());
      } catch (AlreadyExistsException ignored) {
        // created concurrently
      }
    }
    return TableLoader.fromCatalog(loader, id);
  }
}
