package tinydb.storage;

import tinydb.common.DbException;
import tinydb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/** 处理 hasNext()/next() 逻辑*/
public abstract class AbstractDbFileIterator implements DbFileIterator {

	@Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (next == null) {
            next = readNext();
        }
        return next != null;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (next == null) {
            next = readNext();
            if (next == null) {
                throw new NoSuchElementException();
            }
        }

        Tuple result = next;
        next = null;
        return result;
    }

    @Override
    public void close() {
        next = null;
    }

    protected abstract Tuple readNext() throws DbException, TransactionAbortedException;

    private Tuple next = null;
}
