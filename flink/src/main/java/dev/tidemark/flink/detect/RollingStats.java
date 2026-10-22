package dev.tidemark.flink.detect;

/**
 * Fixed-size ring buffer with running mean and standard deviation. Kept as a
 * POJO so it lives in Flink keyed state.
 */
public class RollingStats {
  public double[] values;
  public int next;
  public int count;
  public double sum;
  public double sumSq;

  public RollingStats() {}

  public RollingStats(int capacity) {
    values = new double[capacity];
  }

  public void add(double v) {
    if (count == values.length) {
      double old = values[next];
      sum -= old;
      sumSq -= old * old;
    } else {
      count++;
    }
    values[next] = v;
    next = (next + 1) % values.length;
    sum += v;
    sumSq += v * v;
  }

  public double mean() {
    return count == 0 ? 0 : sum / count;
  }

  public double std() {
    if (count < 2) {
      return 0;
    }
    double m = mean();
    double var = Math.max(0, sumSq / count - m * m);
    return Math.sqrt(var);
  }

  /** z-score of v against the current window (not including v). */
  public double z(double v) {
    double s = std();
    return s == 0 ? (v == mean() ? 0 : Double.POSITIVE_INFINITY) : (v - mean()) / s;
  }
}
