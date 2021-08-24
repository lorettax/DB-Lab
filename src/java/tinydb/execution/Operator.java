package tinydb.execution;

import tinydb.transaction.TransactionAbortedException;
import tinydb.common.DbException;
import tinydb.storage.Tuple;
import tinydb.storage.TupleDesc;

import java.util.NoSuchElementException;

/**
 * Abstract class for implementing operators.
 */
public abstract class Operator implements OpIterator {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!this.open) {
            throw new IllegalStateException("Operator not yet open");
        }
        
        if (next == null) {
            next = fetchNext();
        }
        return next != null;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (next == null) {
            next = fetchNext();
            if (next == null) {
                throw new NoSuchElementException();
            }
        }

        Tuple result = next;
        next = null;
        return result;
    }

    /**
     * Returns the next Tuple in the iterator, or null if the iteration is finished.
     * 
     * @return the next Tuple in the iterator, or null if the iteration is
     *         finished.
     */
    protected abstract Tuple fetchNext() throws DbException,
            TransactionAbortedException;

    /**
     * Closes this iterator. If overridden by a subclass, they should call
     * super.close() in order for Operator's internal state to be consistent.
     */
    @Override
    public void close() {
        // Ensures that a future call to next() will fail
        next = null;
        this.open = false;
    }

    private Tuple next = null;
    private boolean open = false;
    private int estimatedCardinality = 0;

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.open = true;
    }

    /**
     * @return return the children DbIterators of this operator.
     * */
    public abstract OpIterator[] getChildren();

    /**
     * Set the children(child) of this operator.
     *
     * @param children the DbIterators which are to be set as the children(child) of this operator
     * */
    public abstract void setChildren(OpIterator[] children);

    /**
     * @return return the TupleDesc of the output tuples of this operator
     * */
    @Override
    public abstract TupleDesc getTupleDesc();

    /**
     * @return The estimated cardinality of this operator.
     * */
    public int getEstimatedCardinality() {
        return this.estimatedCardinality;
    }

    /**
     * @param card
     *            The estimated cardinality of this operator
     * */
    public void setEstimatedCardinality(int card) {
        this.estimatedCardinality = card;
    }

}
