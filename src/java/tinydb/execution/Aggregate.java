package tinydb.execution;

import tinydb.common.DbException;
import tinydb.common.Type;
import tinydb.storage.Tuple;
import tinydb.storage.TupleDesc;
import tinydb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * 计算聚合的聚合运算符（例如，sum、avg、max、min）。仅支持单列聚合 按单列分组
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;


    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;

    private Aggregator aggregator;
    private OpIterator it;
    private TupleDesc td;

    /**
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        Type gfieldtype = gfield == -1 ? null : this.child.getTupleDesc().getFieldType(this.gfield);

        if(this.child.getTupleDesc().getFieldType(this.afield) == (Type.STRING_TYPE)){
            this.aggregator = new StringAggregator(this.gfield,gfieldtype,this.afield,this.aop);
        }else{
            this.aggregator = new IntegerAggregator(this.gfield,gfieldtype,this.afield,this.aop);
        }
        this.it = this.aggregator.iterator();
        // create tupleDesc for agg
        List<Type> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        // group field
        if (gfieldtype != null) {
            types.add(gfieldtype);
            names.add(this.child.getTupleDesc().getFieldName(this.gfield));
        }
        types.add(this.child.getTupleDesc().getFieldType(this.afield));
        names.add(this.child.getTupleDesc().getFieldName(this.afield));
        if (aop.equals(Aggregator.Op.SUM_COUNT)) {
            types.add(Type.INT_TYPE);
            names.add("COUNT");
        }
        assert (types.size() == names.size());
        this.td = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples.
     */
    public int groupField() {
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        return this.td.getFieldName(0);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        if(this.gfield == -1) {
            return this.td.getFieldName(0);
        } else {
            return this.td.getFieldName(1);
        }

    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    @Override
    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        this.child.open();
        while (this.child.hasNext()) {
            this.aggregator.mergeTupleIntoGroup(this.child.next());
        }
        this.it.open();
        super.open();
    }

    /**
     return 下一个 tuple。如果有group by字段，那么第一个字段就是我们分组的字段，第二个字段是计算聚合的结果。
     如果没有 group by 字段，则结果 tuple 应包含一个字段，表示聚合结果。如果没有更多元组，则应返回 null。
     格式大概为（guoupKey, groupValue） 或者 groupValue
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        while (this.it.hasNext()) {
            return this.it.next();
        }
        return null;

    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
        this.it.rewind();
    }

    /**
     * 返回此聚合的 TupleDesc。如果没有 group by 字段，这将有一个字段 - 聚合列
     */
    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    @Override
    public void close() {
        super.close();
        this.child.close();
        this.it.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
        List<Type> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        Type gfieldtype = gfield == -1 ? null : this.child.getTupleDesc().getFieldType(this.gfield);
        // group field
        if (gfieldtype != null) {
            types.add(gfieldtype);
            names.add(this.child.getTupleDesc().getFieldName(this.gfield));
        }
        types.add(this.child.getTupleDesc().getFieldType(this.afield));
        names.add(this.child.getTupleDesc().getFieldName(this.afield));
        if (aop.equals(Aggregator.Op.SUM_COUNT)) {
            types.add(Type.INT_TYPE);
            names.add("COUNT");
        }
        assert (types.size() == names.size());
        this.td = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));
    }

}
