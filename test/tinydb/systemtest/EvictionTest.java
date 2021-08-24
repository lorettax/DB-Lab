package tinydb.systemtest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;

import org.junit.Test;

import org.junit.Assert;
import tinydb.common.Database;
import tinydb.common.DbException;
import tinydb.common.Utility;
import tinydb.execution.Insert;
import tinydb.execution.SeqScan;
import tinydb.storage.*;
import tinydb.transaction.Transaction;
import tinydb.transaction.TransactionAbortedException;
import tinydb.transaction.TransactionId;


public class EvictionTest extends SimpleDbTestBase {
    private static final long MEMORY_LIMIT_IN_MB = 5;
    private static final int BUFFER_PAGES = 16;

    @Test public void testHeapFileScanWithManyPages() throws IOException, DbException, TransactionAbortedException {
        System.out.println("EvictionTest creating large table");
        HeapFile f = SystemTestUtil.createRandomHeapFile(2, 1024*500, null, null);
        System.out.println("EvictionTest scanning large table");
        Database.resetBufferPool(BUFFER_PAGES);
        long beginMem = SystemTestUtil.getMemoryFootprint();
        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid, f.getId(), "");
        scan.open();
        while (scan.hasNext()) {
            scan.next();
        }
        System.out.println("EvictionTest scan complete, testing memory usage of scan");
        long endMem = SystemTestUtil.getMemoryFootprint();
        long memDiff = (endMem - beginMem) / (1<<20);
        if (memDiff > MEMORY_LIMIT_IN_MB) {
            Assert.fail("Did not evict enough pages.  Scan took " + memDiff + " MB of RAM, when limit was " + MEMORY_LIMIT_IN_MB);
        }
    }

    public static void insertRow(HeapFile f, Transaction t) throws DbException,
            TransactionAbortedException {
        // Create a row to insert
        TupleDesc twoIntColumns = Utility.getTupleDesc(2);
        Tuple value = new Tuple(twoIntColumns);
        value.setField(0, new IntField(-42));
        value.setField(1, new IntField(-43));
        TupleIterator insertRow = new TupleIterator(Utility.getTupleDesc(2), Collections.singletonList(value));

        // Insert the row
        Insert insert = new Insert(t.getId(), insertRow, f.getId());
        insert.open();
        Tuple result = insert.next();
        assertEquals(SystemTestUtil.SINGLE_INT_DESCRIPTOR, result.getTupleDesc());
        assertEquals(1, ((IntField)result.getField(0)).getValue());
        assertFalse(insert.hasNext());
        insert.close();
    }

    public static boolean findMagicTuple(HeapFile f, Transaction t)
            throws DbException, TransactionAbortedException {
        SeqScan ss = new SeqScan(t.getId(), f.getId(), "");
        boolean found = false;
        ss.open();
        while (ss.hasNext()) {
            Tuple v = ss.next();
            int v0 = ((IntField)v.getField(0)).getValue();
            int v1 = ((IntField)v.getField(1)).getValue();
            if (v0 == -42 && v1 == -43) {
                assertFalse(found);
                found = true;
            }
        }
        ss.close();
        return found;
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(EvictionTest.class);
    }
}
