package dev.tidemark.flink.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Minimal schema-registry client: register market.proto under a subject and return its id. */
public final class SchemaRegistry {
  private SchemaRegistry() {}

  public static String marketProto() throws IOException {
    try (InputStream in = SchemaRegistry.class.getResourceAsStream("/tidemark/v1/market.proto")) {
      if (in == null) {
        throw new IOException("market.proto not on the classpath");
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  static String jsonString(String s) {
    StringBuilder b = new StringBuilder("\"");
    for (char c : s.toCharArray()) {
      switch (c) {
        case '"' -> b.append("\\\"");
        case '\\' -> b.append("\\\\");
        case '\n' -> b.append("\\n");
        case '\r' -> b.append("\\r");
        case '\t' -> b.append("\\t");
        default -> b.append(c);
      }
    }
    return b.append('"').toString();
  }

  /** Registers (idempotently) and returns the schema id. */
  public static int register(String registryUrl, String subject, String schema) throws IOException, InterruptedException {
    String body = "{\"schemaType\":\"PROTOBUF\",\"schema\":" + jsonString(schema) + "}";
    HttpRequest req = HttpRequest.newBuilder(URI.create(registryUrl + "/subjects/" + subject + "/versions"))
        .timeout(Duration.ofSeconds(10))
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build();
    HttpResponse<String> resp = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() != 200) {
      throw new IOException("register " + subject + ": HTTP " + resp.statusCode() + " " + resp.body());
    }
    Matcher m = Pattern.compile("\"id\"\\s*:\\s*(\\d+)").matcher(resp.body());
    if (!m.find()) {
      throw new IOException("register " + subject + ": no id in " + resp.body());
    }
    return Integer.parseInt(m.group(1));
  }
}
