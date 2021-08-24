package tinydb.execution;

import tinydb.transaction.TransactionAbortedException;
import tinydb.common.DbException;
import tinydb.storage.Tuple;
import tinydb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
    private Tuple t;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        t = null;
    }

    public JoinPredicate getJoinPredicate() {
        return p;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(child1.getTupleDesc(),child2.getTupleDesc());
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child1.open();
        child2.open();
        super.open();
    }

    @Override
    public void close() {
        super.close();
        child2.close();
        child1.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
    }

    /**
     * 返回连接生成的下一个元组，如果没有更多元组，则返回 null。
     * 从逻辑上讲，这是 r1 跨 r2 中满足连接谓词的下一个元组。有许多可能的实现；最简单的是嵌套循环连接
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {

        while(this.child1.hasNext() || this.t != null){
            if(this.child1.hasNext() && this.t == null){
                t = child1.next();
            }
            while(child2.hasNext()){
                Tuple t2 = child2.next();
                if(p.filter(t,t2)){
                    TupleDesc td1 = t.getTupleDesc();
                    TupleDesc td2 = t2.getTupleDesc();
                    TupleDesc newTd = TupleDesc.merge(td1,td2);
                    Tuple newTuple = new Tuple(newTd);
                    newTuple.setRecordId(t.getRecordId());
                    int i=0;
                    for(;i<td1.numFields();++i) {
                        newTuple.setField(i,t.getField(i));
                    }
                    for(int j=0;j<td2.numFields();++j) {
                        newTuple.setField(i+j,t2.getField(j));
                    }
                    if(!child2.hasNext()){
                        child2.rewind();
                        t = null;
                    }
                    return newTuple;
                }
            }
            child2.rewind();
            t = null;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {child1,child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child1 = children[0];
        child2 = children[1];
    }

}
