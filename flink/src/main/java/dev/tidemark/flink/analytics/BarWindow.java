package dev.tidemark.flink.analytics;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Turns a window's accumulator into a {@link Bar}. With allowed lateness the
 * window fires again for each late trade; each firing carries a higher
 * revision and the sink upserts, so the stored bar converges to the truth.
 */
public class BarWindow extends ProcessWindowFunction<BarAccumulator, Bar, String, TimeWindow> {
  private final String interval;

  public BarWindow(String interval) {
    this.interval = interval;
  }

  @Override
  public void process(String key, Context ctx, Iterable<BarAccumulator> accs, Collector<Bar> out) throws Exception {
    BarAccumulator a = accs.iterator().next();
    if (a.count == 0) {
      return;
    }
    ValueState<Long> fired = ctx.windowState().getState(new ValueStateDescriptor<>("fired", Types.LONG));
    long revision = (fired.value() == null ? 0 : fired.value()) + 1;
    fired.update(revision);

    int sep = key.indexOf(':');
    Bar b = new Bar();
    b.venue = key.substring(0, sep);
    b.productId = key.substring(sep + 1);
    b.interval = interval;
    b.barStartMs = ctx.window().getStart();
    b.open = a.open;
    b.high = a.high;
    b.low = a.low;
    b.close = a.close;
    b.volume = a.volume;
    b.notional = a.notional;
    b.vwap = a.vwap();
    b.buyVolume = a.buyVolume;
    b.sellVolume = a.sellVolume;
    b.tradeCount = a.count;
    b.revision = revision;
    out.collect(b);
  }

  @Override
  public void clear(Context ctx) throws Exception {
    ctx.windowState().getState(new ValueStateDescriptor<>("fired", Types.LONG)).clear();
  }
}
