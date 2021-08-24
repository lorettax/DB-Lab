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
 * 将从子运算符读取的元组插入构造函数中指定的 tableId
 * */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;


    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private final TupleDesc td;

    // helper for fetchNext
    private int counter;
    private boolean called;


    /**
     *
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException
     *          if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))){
            throw new DbException("TupleDesc does not match!");
        }
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"number of inserted tuples"});
        this.counter = -1;
        this.called = false;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;

    }

    @Override
    public void open() throws DbException, TransactionAbortedException {

        this.counter = 0;
        this.child.open();
        super.open();
    }

    @Override
    public void close() {
        super.close();
        this.child.close();
        this.counter = -1;
        this.called = false;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
        this.counter = 0;
        this.called = false;
    }

    /**
     将从 child 读取的元组插入到构造函数指定的 tableId 中
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
                Database.getBufferPool().insertTuple(this.tid, this.tableId, t);
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
