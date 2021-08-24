
package tinydb.storage;

import tinydb.common.DbException;
import tinydb.common.Catalog;
import tinydb.transaction.TransactionAbortedException;
import tinydb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 磁盘上数据库文件的接口。每个表由单个 DbFile 表示。 DbFiles 可以获取页面并遍历元组。
 每个文件都有一个唯一的 id，用于存储关于目录中表的元数据。
 DbFiles 通常通过缓冲池访问
 */
public interface DbFile {
    /**
     * Read the specified page from disk.
     *
     * @throws IllegalArgumentException if the page does not exist in this file.
     */
    Page readPage(PageId id);

    /**
     * Push the specified page to disk.
     *
     * @param p page.getId().pageno() 指定应该写入页面的文件的偏移量
     *
     */
    void writePage(Page p) throws IOException;

    /**
     * 代表事务将指定的元组插入文件。此方法将在文件的受影响页面上获取锁，并且可能会阻塞，直到可以获取锁
     */
    List<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException;

    /**
     * 代表指定的事务从文件中删除指定的元组
     * 此方法将在文件的受影响页面上获取锁，并且可能会阻塞，直到可以获取锁。
     */
    List<Page> deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException;


    DbFileIterator iterator(TransactionId tid);

    int getId();
    
    /**
     * 返回存储在此 DbFile 中的表的 TupleDesc
     */
    TupleDesc getTupleDesc();
}
