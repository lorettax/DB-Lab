package tinydb.systemtest;

import static org.junit.Assert.*;

import tinydb.common.DbException;
import tinydb.execution.Filter;
import tinydb.execution.Predicate;
import tinydb.execution.SeqScan;
import tinydb.storage.HeapFile;
import tinydb.transaction.TransactionAbortedException;
import tinydb.transaction.TransactionId;

public class FilterTest extends FilterBase {
    @Override
    protected int applyPredicate(HeapFile table, TransactionId tid, Predicate predicate)
            throws DbException, TransactionAbortedException {
        SeqScan ss = new SeqScan(tid, table.getId(), "");
        Filter filter = new Filter(predicate, ss);
        filter.open();

        int resultCount = 0;
        while (filter.hasNext()) {
            assertNotNull(filter.next());
            resultCount += 1;
        }

        filter.close();
        return resultCount;
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(FilterTest.class);
    }
}
