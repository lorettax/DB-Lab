package tinydb.execution;

import tinydb.common.Database;
import tinydb.common.DbException;
import tinydb.common.Type;
import tinydb.storage.BufferPool;
import tinydb.storage.IntField;
import tinydb.storage.Tuple;
import tinydb.storage.TupleDesc;
import tinydb.transaction.TransactionAbortedException;
import tinydb.transaction.TransactionId;

import java.io.IOException;

/**
 * 删除运算符 : Delete 从其子操作符中读取元组并将它们从它们所属的表中删除。
 * */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;
    private final TupleDesc td;
    private int counter;
    private boolean called;

    /**
     * 构造函数指定此删除所属的事务以及要从中读取的子项
     * 
     * @param t The transaction this delete runs in
     * @param child 从中读取 tuple 以进行删除的子运算符
     */
    public Delete(TransactionId t, OpIterator child) {

        this.tid = t;
        this.child = child;
        this.td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"number of deleted tuples"});
        this.counter = -1;
        this.called = false;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.child.open();
        super.open();
        this.counter = 0;
    }

    @Override
    public void close() {
        super.close();
        this.child.close();
        this.counter = -1;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
        this.counter = 0;
    }

    /**
     * 删除从子运算符读取的元组。删除是通过缓冲池处理的（可以通过 Database.get BufferPool() 方法访问
     * 
     * @return A 1-field tuple containing the number of deleted records.
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.called) {
            return null;
        }

        this.called = true;
        while (this.child.hasNext()) {
            Tuple t = this.child.next();
            try {
                Database.getBufferPool().deleteTuple(this.tid, t);
                this.counter++;
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }
        Tuple tu = new Tuple(this.td);
        tu.setField(0, new IntField(this.counter));
        return tu;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }

}
