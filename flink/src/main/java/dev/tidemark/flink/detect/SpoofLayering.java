package dev.tidemark.flink.detect;

import dev.tidemark.flink.model.BookMsg;
import dev.tidemark.flink.model.TradeEvent;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Spoof-like layering on one venue's book: a large quantity is added at one of
 * the top {@code nearLevels} levels, then pulled within {@code maxLifeMs}
 * while almost none of it traded.
 *
 * <p>"Large" is the added quantity as a share of the other visible depth on
 * that side (the score): an add of 0.5 is half as big as everything else
 * resting in the top levels. Spoofing works by distorting visible depth, and
 * a share-of-depth yardstick is not fooled by dust levels the way a median
 * level size is (evaluation found most early false positives were ordinary
 * orders scored against 0.002 BTC dust). "Pulled" means the
 * level's quantity fell back by at least {@code pulledFraction} of the add.
 * "Almost none traded" uses price-time priority: a new order joins the back
 * of the queue at its price, so trades there only reach it once the quantity
 * resting ahead of it is used up. The order counts as filled by
 * max(0, traded at the level - quantity ahead of it), which must stay below
 * {@code maxTradedFraction} of the add.
 *
 * <p>Two filters remove normal market making, found by evaluating on live
 * Kraken data: an order pulled sooner than {@code minLifeMs} is quote flicker,
 * and a pull followed within {@code repriceMs} by a similar-sized add on the
 * same side near the top is an amend (the order moved), not liquidity leaving.
 * A pull is therefore only confirmed {@code repriceMs} after it happens.
 *
 * <p>The local book is truncated to {@code depth} levels per side after every
 * message, as Kraken requires: levels that drift out of the subscribed depth
 * are never deleted by the feed, and without truncation they resurface later
 * as phantom best prices.
 *
 * <p>Public feeds carry no order or trader identity, so this flags a
 * pattern, not intent; the investigator agent decides what it looks like.
 */
public class SpoofLayering extends KeyedCoProcessFunction<String, BookMsg, TradeEvent, AlertEvent> {
  public static final String NAME = "spoof_layering";

  private final int nearLevels;
  private final double sizeMultiple;
  private final long maxLifeMs;
  private final double pulledFraction;
  private final double maxTradedFraction;
  private final long minLifeMs;
  private final long repriceMs;
  private final int depth;

  private transient MapState<String, BigDecimal> bids;
  private transient MapState<String, BigDecimal> asks;
  private transient MapState<String, Candidate> candidates; // key: side + "@" + price

  /** A large add being watched. */
  public static class Candidate {
    public String side;
    public String price;
    public double added;
    public double levelBefore;
    public double tradedAtLevel;
    public long addTsMs;
    public int rank;
    public double medianLevel;
    public long pulledTsMs; // 0 while resting
    public double remaining;

    public Candidate() {}
  }

  public SpoofLayering(int nearLevels, double sizeMultiple, long maxLifeMs, double pulledFraction,
      double maxTradedFraction, long minLifeMs, long repriceMs, int depth) {
    this.depth = depth;
    this.minLifeMs = minLifeMs;
    this.repriceMs = repriceMs;
    this.nearLevels = nearLevels;
    this.sizeMultiple = sizeMultiple;
    this.maxLifeMs = maxLifeMs;
    this.pulledFraction = pulledFraction;
    this.maxTradedFraction = maxTradedFraction;
  }

  @Override
  public void open(Configuration parameters) {
    bids = getRuntimeContext().getMapState(new MapStateDescriptor<>("bids", Types.STRING, TypeInformation.of(BigDecimal.class)));
    asks = getRuntimeContext().getMapState(new MapStateDescriptor<>("asks", Types.STRING, TypeInformation.of(BigDecimal.class)));
    candidates = getRuntimeContext().getMapState(
        new MapStateDescriptor<>("candidates", Types.STRING, TypeInformation.of(Candidate.class)));
  }

  static String norm(String price) {
    return new BigDecimal(price).stripTrailingZeros().toPlainString();
  }

  @Override
  public void processElement1(BookMsg m, Context ctx, Collector<AlertEvent> out) throws Exception {
    long ts = m.eventTsUs / 1000;
    if (m.isSnapshot) {
      bids.clear();
      asks.clear();
      candidates.clear();
    }
    apply(m.bids, bids, "bid", m.isSnapshot, ts, ctx, out);
    apply(m.asks, asks, "ask", m.isSnapshot, ts, ctx, out);
    truncate(bids, true);
    truncate(asks, false);
  }

  private void truncate(MapState<String, BigDecimal> side, boolean bid) throws Exception {
    List<BigDecimal> prices = new ArrayList<>();
    for (String p : side.keys()) {
      prices.add(new BigDecimal(p));
    }
    if (prices.size() <= depth) {
      return;
    }
    prices.sort(bid ? java.util.Comparator.reverseOrder() : java.util.Comparator.naturalOrder());
    for (BigDecimal p : prices.subList(depth, prices.size())) {
      side.remove(p.stripTrailingZeros().toPlainString());
    }
  }

  private void apply(List<BookMsg.Level> levels, MapState<String, BigDecimal> side, String sideName, boolean snapshot,
      long ts, Context ctx, Collector<AlertEvent> out) throws Exception {
    for (BookMsg.Level l : levels) {
      String price = norm(l.price);
      BigDecimal qty = new BigDecimal(l.qty);
      BigDecimal before = side.get(price);
      double prev = before == null ? 0 : before.doubleValue();
      double now = qty.doubleValue();
      if (qty.signum() == 0) {
        side.remove(price);
      } else {
        side.put(price, qty);
      }
      if (snapshot) {
        continue;
      }
      String key = sideName + "@" + price;
      Candidate c = candidates.get(key);
      if (c != null && c.pulledTsMs == 0 && now <= c.levelBefore + c.added * (1 - pulledFraction)) {
        long life = ts - c.addTsMs;
        double filled = Math.max(0, c.tradedAtLevel - c.levelBefore);
        if (filled >= maxTradedFraction * c.added || life > maxLifeMs || life < minLifeMs) {
          candidates.remove(key); // traded against, rested long enough, or just flicker
        } else {
          c.pulledTsMs = ts;
          c.remaining = now;
          candidates.put(key, c);
          ctx.timerService().registerEventTimeTimer(ts + repriceMs);
        }
        continue;
      }
      if (now > prev) {
        double added = now - prev;
        int rank = rank(side, price, sideName.equals("bid"));
        if (rank <= nearLevels + 2) {
          cancelRepriced(sideName, price, added, ts);
        }
      }
      if (c == null && now > prev) {
        double added = now - prev;
        int rank = rank(side, price, sideName.equals("bid"));
        double depthOthers = depthExcluding(side, price);
        if (rank <= nearLevels && depthOthers > 0 && added >= sizeMultiple * depthOthers) {
          Candidate n = new Candidate();
          n.side = sideName;
          n.price = price;
          n.added = added;
          n.levelBefore = prev;
          n.addTsMs = ts;
          n.rank = rank;
          n.medianLevel = depthOthers;
          candidates.put(key, n);
          ctx.timerService().registerEventTimeTimer(ts + maxLifeMs + 1);
        }
      }
    }
  }

  @Override
  public void processElement2(TradeEvent t, Context ctx, Collector<AlertEvent> out) throws Exception {
    if (t.price == null || t.size == null || candidates.isEmpty()) {
      return;
    }
    String price = norm(t.price.toPlainString());
    for (String side : new String[] {"bid", "ask"}) {
      Candidate c = candidates.get(side + "@" + price);
      if (c != null) {
        c.tradedAtLevel += t.size.doubleValue();
        candidates.put(side + "@" + price, c);
      }
    }
  }

  /** A similar-sized add on the same side shortly after a pull means the order moved, not left. */
  private void cancelRepriced(String side, String price, double added, long ts) throws Exception {
    List<String> moved = new ArrayList<>();
    for (Map.Entry<String, Candidate> e : candidates.entries()) {
      Candidate c = e.getValue();
      if (c.pulledTsMs > 0 && c.side.equals(side) && !c.price.equals(price)
          && ts - c.pulledTsMs <= repriceMs && added >= pulledFraction * c.added) {
        moved.add(e.getKey());
      }
    }
    for (String k : moved) {
      candidates.remove(k);
    }
  }

  @Override
  public void onTimer(long ts, OnTimerContext ctx, Collector<AlertEvent> out) throws Exception {
    List<String> done = new ArrayList<>();
    for (Map.Entry<String, Candidate> e : candidates.entries()) {
      Candidate c = e.getValue();
      if (c.pulledTsMs > 0 && c.pulledTsMs + repriceMs <= ts) {
        out.collect(alert(ctx.getCurrentKey(), c, c.pulledTsMs, c.remaining)); // pull confirmed
        done.add(e.getKey());
      } else if (c.pulledTsMs == 0 && c.addTsMs + maxLifeMs < ts) {
        done.add(e.getKey()); // stayed on the book: not spoof-like
      }
    }
    for (String k : done) {
      candidates.remove(k);
    }
  }

  private AlertEvent alert(String key, Candidate c, long pulledTs, double remaining) {
    int sep = key.indexOf(':');
    double multiple = c.added / c.medianLevel; // share of the other visible depth
    AlertEvent a = AlertEvent.of(NAME, key.substring(0, sep), key.substring(sep + 1), c.addTsMs, pulledTs, pulledTs,
        multiple, sizeMultiple);
    a.summary = String.format("%s %s: %.4f added at %s %s (level %d, %.2fx the rest of the visible depth) and pulled after %d ms with %.4f traded",
        a.venue, a.productId, c.added, c.side, c.price, c.rank, multiple, pulledTs - c.addTsMs, c.tradedAtLevel);
    a.put("side", c.side).put("price", c.price).put("added_qty", c.added).put("level_before", c.levelBefore)
        .put("remaining_after_pull", remaining).put("traded_at_level", c.tradedAtLevel)
        .put("filled_estimate", Math.max(0, c.tradedAtLevel - c.levelBefore)).put("rank", c.rank)
        .put("other_visible_depth", c.medianLevel).put("lifetime_ms", pulledTs - c.addTsMs);
    return a;
  }

  static int rank(MapState<String, BigDecimal> side, String price, boolean bid) throws Exception {
    BigDecimal p = new BigDecimal(price);
    int better = 0;
    for (String other : side.keys()) {
      int cmp = new BigDecimal(other).compareTo(p);
      if (bid ? cmp > 0 : cmp < 0) {
        better++;
      }
    }
    return better + 1;
  }

  /** Total quantity on the other visible levels of this side. */
  static double depthExcluding(MapState<String, BigDecimal> side, String excludePrice) throws Exception {
    double sum = 0;
    for (Map.Entry<String, BigDecimal> e : side.entries()) {
      if (!e.getKey().equals(excludePrice)) {
        sum += e.getValue().doubleValue();
      }
    }
    return sum;
  }


}
