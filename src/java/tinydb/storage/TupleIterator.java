package tinydb.storage;

import tinydb.execution.OpIterator;

import java.util.*;

/**
 * 通过包装 Iterable<Tuple> 实现 OpIterator.
 */
public class TupleIterator implements OpIterator {
    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;
    Iterator<Tuple> i = null;
    TupleDesc td = null;
    Iterable<Tuple> tuples = null;

    /**
     * 从指定的 Iterable 和指定的 Descriptor 构造一个 iterator
     * 
     * @param tuples The set of tuples to iterate over
     */
    public TupleIterator(TupleDesc td, Iterable<Tuple> tuples) {
        this.td = td;
        this.tuples = tuples;

        // check that all tuples are the right TupleDesc
        for (Tuple t : tuples) {
            if (!t.getTupleDesc().equals(td)) {
                throw new IllegalArgumentException(
                        "incompatible tuple in tuple set");
            }
        }
    }

    @Override
    public void open() {
        i = tuples.iterator();
    }

    @Override
    public boolean hasNext() {
        return i.hasNext();
    }

    @Override
    public Tuple next() {
        return i.next();
    }

    @Override
    public void rewind() {
        close();
        open();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public void close() {
        i = null;
    }
}
