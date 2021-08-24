package tinydb.execution;
import tinydb.transaction.TransactionAbortedException;
import tinydb.common.DbException;
import tinydb.storage.Tuple;
import tinydb.storage.TupleDesc;

import java.io.Serializable;
import java.util.*;

/**
 * OpIterator is the iterator interface that all DB operators should implement.
 */
public interface OpIterator extends Serializable{
  /**
   * Opens the iterator. This must be called before any of the other methods.
   */
  void open()
      throws DbException, TransactionAbortedException;

  /** Returns true if the iterator has more tuples.
   * @return true f the iterator has more tuples.
 */
  boolean hasNext() throws DbException, TransactionAbortedException;

  /**
   * Returns the next tuple from the operator (typically implementing by reading
   * from a child operator or an access method).
   *
   * @return the next tuple in the iteration.
   */
  Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException;

  /**
   * Resets the iterator to the start.
   */
  void rewind() throws DbException, TransactionAbortedException;

  /**
   * Returns the TupleDesc associated with this OpIterator.
   * @return the TupleDesc associated with this OpIterator.
   */
  TupleDesc getTupleDesc();

  /**
   * Closes the iterator. When the iterator is closed, calling next(),
   * hasNext(), or rewind() should fail by throwing IllegalStateException.
   */
  void close();

}
