package tinydb;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;
import tinydb.common.Utility;
import tinydb.execution.Insert;
import tinydb.execution.OpIterator;
import tinydb.storage.TupleDesc;
import tinydb.transaction.TransactionId;


public class InsertTest extends TestUtil.CreateHeapFile {

  private OpIterator scan1;
  private TransactionId tid;

  /**
   * Initialize each unit test
   */
  @Before public void setUp() throws Exception {
    super.setUp();
    this.scan1 = TestUtil.createTupleList(2,
        new int[] { 1, 2,
                    1, 4,
                    1, 6,
                    3, 2,
                    3, 4,
                    3, 6,
                    5, 7 });
    tid = new TransactionId();
  }

  /**
   * Unit test for Insert.getTupleDesc()
   */
  @Test public void getTupleDesc() throws Exception {
    Insert op = new Insert(tid,scan1, empty.getId());
    TupleDesc expected = Utility.getTupleDesc(1);
    TupleDesc actual = op.getTupleDesc();
    assertEquals(expected, actual);
  }

  /**
   * Unit test for Insert.getNext(), inserting elements into an empty file
   */
  @Test public void getNext() throws Exception {
    Insert op = new Insert(tid,scan1, empty.getId());
    op.open();
    assertTrue(TestUtil.compareTuples(
        Utility.getHeapTuple(7, 1), // the length of scan1
        op.next()));

    // we should fit on one page
    assertEquals(1, empty.numPages());
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(InsertTest.class);
  }
}

