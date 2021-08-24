package tinydb.execution;

import tinydb.common.Database;
import tinydb.transaction.TransactionAbortedException;
import tinydb.transaction.TransactionId;
import tinydb.common.DbException;
import tinydb.storage.DbFileIterator;
import tinydb.storage.Tuple;
import tinydb.storage.TupleDesc;

import java.util.*;

/**
 * * SeqScan 是一种顺序扫描访问方法的实现，它以没有特定顺序（例如，按照它们在 disk 上的布局）读取 table 的每个 tuple
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private int tableId;
    private String tableAlias;
    private DbFileIterator it;

    /**
     * 在指定的表上创建顺序扫描作为指定事务的一部分
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {

        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }


    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }


    public String getAlias()
    {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.
     */
    public void reset(int tableid, String tableAlias) {

        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        it = Database.getCatalog().getDatabaseFile(tableId).iterator(tid);
        it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return Database.getCatalog().getTupleDesc(tableId);
    }

    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        if(it == null){
            return false;
        }
        return it.hasNext();

    }

    @Override
    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {

        if(it == null){
            throw new NoSuchElementException("no next tuple");
        }
        Tuple t = it.next();
        if(t == null){
            throw new NoSuchElementException("no next tuple");
        }
        return t;
    }

    @Override
    public void close() {
        it = null;
    }

    @Override
    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        it.rewind();
    }
}
