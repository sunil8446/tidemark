package dev.tidemark.flink.model;

import java.util.ArrayList;
import java.util.List;

/** A whole book snapshot or update message (not flattened), for book-aware detectors. */
public class BookMsg {
  public String venue;
  public String productId;
  public boolean isSnapshot;
  public List<Level> bids = new ArrayList<>();
  public List<Level> asks = new ArrayList<>();
  public long eventTsUs;

  public BookMsg() {}

  public String instrumentKey() {
    return venue + ":" + productId;
  }

  /** One price level; qty "0" removes it. Kept as text so prices compare exactly. */
  public static class Level {
    public String price;
    public String qty;

    public Level() {}

    public Level(String price, String qty) {
      this.price = price;
      this.qty = qty;
    }
  }
}
