package tinydb.execution;

import tinydb.common.DbException;
import tinydb.common.Type;
import tinydb.storage.*;
import tinydb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * 在一组 StringField 上计算一些聚合
 */
public class StringAggregator implements Aggregator {


    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private Map<Field, Integer> groupMap;


    /**
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {

        if (!what.equals(Op.COUNT)) {
            throw new IllegalArgumentException("Only COUNT is supported for String fields!");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupMap = new HashMap<>();
    }

    /**
     * 将一个new tuple merge into aggreage中，按照构造函数中的指示进行grouping
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        StringField afield = (StringField) tup.getField(this.afield);
        Field gbfield = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        String newValue = afield.getValue();
        if (gbfield != null && gbfield.getType() != this.gbfieldtype) {
            throw new IllegalArgumentException("Given tuple has wrong type");
        }
        if (!this.groupMap.containsKey(gbfield)) {
            this.groupMap.put(gbfield, 1);
        } else {
            this.groupMap.put(gbfield, this.groupMap.get(gbfield) + 1);
        }
    }

    /**
     * 在组聚合结果上创建一个 OpIterator
     */
    @Override
    public OpIterator iterator() {
        return new AggregateIterator(this.groupMap, this.gbfieldtype);
    }


    static class AggregateIterator implements OpIterator {

        protected Iterator<Map.Entry<Field, Integer>> it;
        TupleDesc td;

        private Map<Field, Integer> groupMap;
        protected Type itgbfieldtype;

        public AggregateIterator(Map<Field, Integer> groupMap, Type gbfieldtype) {
            this.groupMap = groupMap;
            this.itgbfieldtype = gbfieldtype;
            // no grouping
            if (this.itgbfieldtype == null) {
                this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
            } else {
                this.td = new TupleDesc(new Type[]{this.itgbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
            }
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.it = groupMap.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Map.Entry<Field, Integer> entry = this.it.next();
            Field f = entry.getKey();
            Tuple rtn = new Tuple(this.td);
            this.setFields(rtn, entry.getValue(), f);
            return rtn;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.it = groupMap.entrySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return this.td;
        }

        @Override
        public void close() {
            this.it = null;
            this.td = null;
        }

        void setFields(Tuple rtn, int value, Field f) {
            if (f == null) {
                rtn.setField(0, new IntField(value));
            } else {
                rtn.setField(0, f);
                rtn.setField(1, new IntField(value));
            }
        }
    }
}
