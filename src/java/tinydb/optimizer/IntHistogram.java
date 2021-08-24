package tinydb.optimizer;

import tinydb.execution.Predicate;

/** 表示基于单个整数字段的固定宽度直方图的类.
 */
public class IntHistogram {

    private final int[] buckets;
    private final int min, max;
    private final double width;
    private int ntups = 0;

    /**
     * 创建一个新的 IntHistogram
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = (1.+max-min)/this.buckets.length;
    }


    private int getIndex(int v){
        if(v<min || v>max) {
            throw new IllegalArgumentException("value out of range");
        }
        return (int)((v-min)/width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if(v >= min && v < max){
            buckets[getIndex(v)]++;
            ntups++;
        }

    }

    /**
     * 估计此表上特定谓词和操作数的选择性
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        if(op.equals(Predicate.Op.LESS_THAN)){
            if(v <= min) {
                return 0.0;
            }
            if(v >= max) {
                return 1.0;
            }
            final int index = getIndex(v);
            double cnt = 0;
            for(int i=0;i<index;++i){
                cnt += buckets[i];
            }
            cnt += buckets[index]/width*(v-index*width-min);
            return cnt/ntups;
        }
        if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            return estimateSelectivity(Predicate.Op.LESS_THAN, v+1);
        }
        if (op.equals(Predicate.Op.GREATER_THAN)) {
            return 1-estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
        }
        if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            return estimateSelectivity(Predicate.Op.GREATER_THAN, v-1);
        }
        if (op.equals(Predicate.Op.EQUALS)) {
            return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) -
                    estimateSelectivity(Predicate.Op.LESS_THAN, v);
        }
        if (op.equals(Predicate.Op.NOT_EQUALS)) {
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return 0.0;

    }
    
    /**
     * @return 该直方图的平均选择性
     *
     * This is not an indispensable method to implement the basic join optimization.
     * */
    public double avgSelectivity()
    {
        int cnt = 0;
        for(int bucket:buckets) {
            cnt += bucket;
        }
        if(cnt ==0) {
            return 0.0;
        }
        return cnt/ntups;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return String.format("IntHistgram(buckets=%d, min=%d, max=%d",
                buckets.length, min, max);
    }
}
