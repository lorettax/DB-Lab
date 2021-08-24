package tinydb.execution;

import tinydb.transaction.TransactionAbortedException;
import tinydb.common.DbException;
import tinydb.storage.Tuple;
import tinydb.storage.TupleDesc;

import java.util.*;

/**
 * 过滤器是一个实现关系选择的运算符
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private final Predicate p;
    private  OpIterator child;


    /**
     * 构造函数接受一个要应用的谓词和一个子运算符来读取要过滤的元组
     * 
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return p;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        super.open();
    }

    @Override
    public void close() {
        super.close();
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * 
     * @return 通过过滤器的 next tuple，如果没有其它 tuple，则为 null
     * @see Predicate#filter
     */
    @Override
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while(child.hasNext()){
            Tuple t = child.next();
            if(p.filter(t)){
                return t;
            }
        }
        return null;

    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}
