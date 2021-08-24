package tinydb.execution;

import tinydb.common.DbException;
import tinydb.common.Type;
import tinydb.execution.StringAggregator.AggregateIterator;
import tinydb.storage.Field;
import tinydb.storage.IntField;
import tinydb.storage.Tuple;
import tinydb.storage.TupleDesc;
import tinydb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    // running SUM,MIN,MAX,COUNT
    private Map<Field,Integer> groupMap;
    private Map<Field,Integer> countMap;
    private Map<Field, List<Integer>> avgMap;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupMap = new HashMap<>();
        this.avgMap = new HashMap<>();
        this.countMap = new HashMap<>();
    }

    /**
     * 将一个new tuple 合并到聚合中，按照构造函数中的指示进行分组
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {

        IntField afield = (IntField)tup.getField(this.afield);
        Field gbfield = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        int newValue = afield.getValue();
        if(gbfield != null && gbfield.getType()!=this.gbfieldtype){
            throw new IllegalArgumentException("Given tuple has wrong type");
        }
        // get number
        switch(this.what){
            case MIN:
                if(!this.groupMap.containsKey(gbfield)) {
                    this.groupMap.put(gbfield,newValue);
                } else {
                    this.groupMap.put(gbfield,Math.min(this.groupMap.get(gbfield),newValue));
                }
                break;

            case MAX:
                if (!this.groupMap.containsKey(gbfield)) {
                    this.groupMap.put(gbfield, newValue);
                } else {
                    this.groupMap.put(gbfield, Math.max(this.groupMap.get(gbfield), newValue));
                }
                break;

            case SUM:
                if (!this.groupMap.containsKey(gbfield)) {
                    this.groupMap.put(gbfield, newValue);
                } else {
                    this.groupMap.put(gbfield, this.groupMap.get(gbfield) + newValue);
                }
                break;

            case COUNT:
                if (!this.groupMap.containsKey(gbfield)) {
                    this.groupMap.put(gbfield, 1);
                } else {
                    this.groupMap.put(gbfield, this.groupMap.get(gbfield) + 1);
                }
                break;

            case SC_AVG:
                IntField countField = null;
                if (gbfield == null) {
                    countField = (IntField)tup.getField(1);
                } else {
                    countField = (IntField)tup.getField(2);
                }
                int countValue = countField.getValue();
                if (!this.groupMap.containsKey(gbfield)) {
                    this.groupMap.put(gbfield, newValue);
                    this.countMap.put(gbfield, countValue);
                } else {
                    this.groupMap.put(gbfield, this.groupMap.get(gbfield) + newValue);
                    this.countMap.put(gbfield, this.countMap.get(gbfield) + countValue);
                }
            case SUM_COUNT:

            case AVG:
                if (!this.avgMap.containsKey(gbfield)) {
                    List<Integer> l = new ArrayList<>();
                    l.add(newValue);
                    this.avgMap.put(gbfield, l);
                } else {
                    // reference
                    List<Integer> l = this.avgMap.get(gbfield);
                    l.add(newValue);
                }
                break;
            default:
                throw new IllegalArgumentException("Aggregate not supported!");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     */
    @Override
    public OpIterator iterator() {
        return new IntAggIterator();
    }


    private class IntAggIterator extends AggregateIterator {

        private Iterator<Map.Entry<Field, List<Integer>>> avgIt;
        private boolean isAvg;
        private boolean isSCAvg;
        private boolean isSumCount;

        IntAggIterator() {
            super(groupMap, gbfieldtype);
            this.isAvg = what.equals(Op.AVG);
            this.isSCAvg = what.equals(Op.SC_AVG);
            this.isSumCount = what.equals(Op.SUM_COUNT);
            if (isSumCount) {
                this.td = new TupleDesc(new Type[] {this.itgbfieldtype, Type.INT_TYPE, Type.INT_TYPE},
                        new String[] {"groupVal", "sumVal", "countVal"});
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            super.open();
            if (this.isAvg || this.isSumCount) {
                this.avgIt = avgMap.entrySet().iterator();
            } else {
                this.avgIt = null;
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.isAvg || this.isSumCount) {
                return avgIt.hasNext();
            }
            return super.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple rtn = new Tuple(td);
            if (this.isAvg || this.isSumCount) {
                Map.Entry<Field, List<Integer>> avgOrSumCountEntry = this.avgIt.next();
                Field avgOrSumCountField = avgOrSumCountEntry.getKey();
                List<Integer> avgOrSumCountList = avgOrSumCountEntry.getValue();
                if (this.isAvg) {
                    int value = this.sumList(avgOrSumCountList) / avgOrSumCountList.size();
                    this.setFields(rtn, value, avgOrSumCountField);
                    return rtn;
                } else {
                    this.setFields(rtn, sumList(avgOrSumCountList), avgOrSumCountField);
                    if (avgOrSumCountField != null) {
                        rtn.setField(2, new IntField(avgOrSumCountList.size()));
                    } else {
                        rtn.setField(1, new IntField(avgOrSumCountList.size()));
                    }
                    return rtn;
                }
            } else if (this.isSCAvg) {
                Map.Entry<Field, Integer> entry = this.it.next();
                Field f = entry.getKey();
                this.setFields(rtn, entry.getValue() / countMap.get(f), f);
                return rtn;
            }
            return super.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            super.rewind();
            if (this.isAvg || this.isSumCount) {
                this.avgIt = avgMap.entrySet().iterator();
            }
        }

        @Override
        public TupleDesc getTupleDesc() {
            return super.getTupleDesc();
        }

        @Override
        public void close() {
            super.close();
            this.avgIt = null;
        }

        private int sumList(List<Integer> l) {
            int sum = 0;
            for (int i : l) {
                sum += i;
            }
            return sum;
        }
    }

}
