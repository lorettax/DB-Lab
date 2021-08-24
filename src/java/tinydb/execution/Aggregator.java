package tinydb.execution;

import tinydb.storage.Tuple;
import tinydb.storage.TupleIterator;

import java.io.Serializable;

/**
 * 任何可以计算 tuple list 聚合的类的通用 interface
 */
public interface Aggregator extends Serializable {
    int NO_GROUPING = -1;


    enum Op implements Serializable {
        MIN, MAX, SUM, AVG, COUNT,

        SUM_COUNT,

        SC_AVG;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }
        
        @Override
        public String toString()
        {
        	if (this==MIN) {
                return "min";
            }
        	if (this==MAX) {
                return "max";
            }
        	if (this==SUM) {
                return "sum";
            }
        	if (this==SUM_COUNT) {
                return "sum_count";
            }
        	if (this==AVG) {
                return "avg";
            }
        	if (this==COUNT) {
                return "count";
            }
        	if (this==SC_AVG) {
                return "sc_avg";
            }
        	throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    void mergeTupleIntoGroup(Tuple tup);

    /**
     * Create a OpIterator over group aggregate results.
     * @see TupleIterator for a possible helper
     */
    OpIterator iterator();
    
}
