package tinydb.storage;
import tinydb.common.DbException;
import tinydb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * DbFileIterator is the iterator interface that all DB Dbfile should implement.
 * DbFileIterator 是所有 DB Dbfile 都应该实现的迭代器接口
 */
public interface DbFileIterator{
    /**
     * Opens the iterator
     */
    void open()
        throws DbException, TransactionAbortedException;

    /** @return 如果有更多元组可用，则为 true，如果没有更多元组或迭代器未打开，则为 false */
    boolean hasNext()
        throws DbException, TransactionAbortedException;

    /**
     * 从运算符获取下一个元组（通常通过从子运算符或访问方法读取来实现）
     * @return The next tuple in the iterator.
     */
    Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException;

    /**
     * Resets the iterator to the start.
     */
    void rewind() throws DbException, TransactionAbortedException;

    /**
     * Closes the iterator.
     */
    void close();
}
