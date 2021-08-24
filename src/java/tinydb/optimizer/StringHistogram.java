package tinydb.optimizer;

import tinydb.execution.Predicate;

/**
 * 在单个基于字符串的字段上表示固定宽度直方图的类
 */
public class StringHistogram {
    final IntHistogram hist;

    /**
     * Create a new StringHistogram with a specified number of buckets.
     * @param buckets the number of buckets
     */
    public StringHistogram(int buckets) {
        hist = new IntHistogram(buckets, minVal(), maxVal());
    }

    /**
     * 将字符串转换为整数，具有如果返回值(s1) < 返回值(s2)，则s1 < s2 的特性
     */
    private int stringToInt(String s) {
        int i;
        int v = 0;
        for (i = 3; i >= 0; i--) {
            if (s.length() > 3 - i) {
                int ci = s.charAt(3 - i);
                v += (ci) << (i * 8);
            }
        }

        // XXX: hack to avoid getting wrong results for
        // strings which don't output in the range min to max
        if (!(s.equals("") || s.equals("zzzz"))) {
            if (v < minVal()) {
                v = minVal();
            }

            if (v > maxVal()) {
                v = maxVal();
            }
        }

        return v;
    }

    /** @return the maximum value indexed by the histogram */
    int maxVal() {
        return stringToInt("zzzz");
    }

    /** @return the minimum value indexed by the histogram */
    int minVal() {
        return stringToInt("");
    }

    /** Add a new value to thte histogram */
    public void addValue(String s) {
        int val = stringToInt(s);
        hist.addValue(val);
    }

    /**
     * 估计指定谓词对指定字符串的选择性（作为 0 和 1 之间的双精度值）
     * 
     * @param op The operation being applied
     * @param s The string to apply op to
     */
    public double estimateSelectivity(Predicate.Op op, String s) {
        int val = stringToInt(s);
        return hist.estimateSelectivity(op, val);
    }

    /**
     * @return the average selectivity of this histogram.
     *
     * */
    public double avgSelectivity() {
        return hist.avgSelectivity();
    }
}
