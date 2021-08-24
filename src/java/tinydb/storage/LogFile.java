
package tinydb.storage;

import com.sun.deploy.util.UpdateCheck;
import tinydb.common.Database;
import tinydb.common.DbException;
import tinydb.common.Permissions;
import tinydb.transaction.TransactionAbortedException;
import tinydb.transaction.TransactionId;
import tinydb.common.Debug;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;


/**
<p> The format of the log file is as follows:

<li> The first long integer of the file represents the offset of the
last written checkpoint, or -1 if there are no checkpoints

<li> All additional data in the log consists of log records.  Log
records are variable length.

<li> Each log record begins with an integer type and a long integer
transaction id.

<li> Each log record ends with a long integer file offset representing
the position in the log file where the record began.

<li> There are five record types: ABORT, COMMIT, UPDATE, BEGIN, and
CHECKPOINT

<li> ABORT, COMMIT, and BEGIN records contain no additional data

<li>UPDATE RECORDS consist of two entries, a before image and an
after image.  These images are serialized Page objects, and can be
accessed with the LogFile.readPageData() and LogFile.writePageData()
methods.  See LogFile.print() for an example.

<li> CHECKPOINT records consist of active transactions at the time
the checkpoint was taken and their first log record on disk.  The format
of the record is an integer count of the number of transactions, as well
as a long integer transaction id and a long integer first record offset
for each active transaction.

</ul>
*/
public class LogFile {

    final File logFile;
    private RandomAccessFile raf;
    Boolean recoveryUndecided; // no call to recover() and no append to log

    static final int ABORT_RECORD = 1;
    static final int COMMIT_RECORD = 2;
    static final int UPDATE_RECORD = 3;
    static final int BEGIN_RECORD = 4;
    static final int CHECKPOINT_RECORD = 5;
    static final long NO_CHECKPOINT_ID = -1;

    final static int INT_SIZE = 4;
    final static int LONG_SIZE = 8;

    long currentOffset = -1;//protected by this
//    int pageSize;
    int totalRecords = 0; // for PatchTest //protected by this

    final Map<Long,Long> tidToFirstLogRecord = new HashMap<>();

    /**
     * @param f The log file's name
    */
    public LogFile(File f) throws IOException {
	this.logFile = f;
        raf = new RandomAccessFile(f, "rw");
        recoveryUndecided = true;
    }

    //about to append a log record. if we weren't sure whether the
    // DB wants to do recovery, we're sure now -- it didn't. So truncate
    // the log.
    void preAppend() throws IOException {
        totalRecords++;
        if(recoveryUndecided){
            recoveryUndecided = false;
            raf.seek(0);
            raf.setLength(0);
            raf.writeLong(NO_CHECKPOINT_ID);
            raf.seek(raf.length());
            currentOffset = raf.getFilePointer();
        }
    }

    public synchronized int getTotalRecords() {
        return totalRecords;
    }
    
    /** Write an abort record to the log for the specified tid, force
        the log to disk, and perform a rollback
        @param tid The aborting transaction.
    */
    public void logAbort(TransactionId tid) throws IOException, TransactionAbortedException, DbException {
        // must have buffer pool lock before proceeding, since this
        // calls rollback

        synchronized (Database.getBufferPool()) {

            synchronized(this) {
                preAppend();
                //Debug.log("ABORT");
                //should we verify that this is a live transaction?

                // must do this here, since rollback only works for
                // live transactions (needs tidToFirstLogRecord)
                rollback(tid);

                raf.writeInt(ABORT_RECORD);
                raf.writeLong(tid.getId());
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                force();
                tidToFirstLogRecord.remove(tid.getId());
            }
        }
    }

    /** Write a commit record to disk for the specified tid,
        and force the log to disk.
        @param tid The committing transaction.
    */
    public synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();
        Debug.log("COMMIT " + tid.getId());
        //should we verify that this is a live transaction?

        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        force();
        tidToFirstLogRecord.remove(tid.getId());
    }

    /** Write an UPDATE record to disk for the specified tid and page
        (with provided         before and after images.)
        @param tid The transaction performing the write
        @param before The before image of the page
        @param after The after image of the page
        @see Page#getBeforeImage
    */
    public  synchronized void logWrite(TransactionId tid, Page before,
                                       Page after)
        throws IOException  {
        Debug.log("WRITE, offset = " + raf.getFilePointer());
        preAppend();
        /* update record conists of

           record type
           transaction id
           before page data (see writePageData)
           after page data
           start offset
        */
        raf.writeInt(UPDATE_RECORD);
        raf.writeLong(tid.getId());

        writePageData(raf,before);
        writePageData(raf,after);
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("WRITE OFFSET = " + currentOffset);
    }

    void writePageData(RandomAccessFile raf, Page p) throws IOException{
        PageId pid = p.getId();
        int[] pageInfo = pid.serialize();

        //page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data

        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int j : pageInfo) {
            raf.writeInt(j);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
        //        Debug.log ("WROTE PAGE DATA, CLASS = " + pageClassName + ", table = " +  pid.getTableId() + ", page = " + pid.pageno());
    }

    Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();

        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object[] idArgs = new Object[numIdArgs];
            for (int i = 0; i<numIdArgs;i++) {
                idArgs[i] = raf.readInt();
            }
            pid = (PageId)idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData); //read before image

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            newPage = (Page)pageConsts[0].newInstance(pageArgs);

            //            Debug.log("READ PAGE OF TYPE " + pageClassName + ", table = " + newPage.getId().getTableId() + ", page = " + newPage.getId().pageno());
        } catch (ClassNotFoundException | InvocationTargetException | IllegalAccessException | InstantiationException e){
            e.printStackTrace();
            throw new IOException();
        }
        return newPage;

    }

    /** Write a BEGIN record for the specified transaction
        @param tid The transaction that is beginning
    */
    public synchronized  void logXactionBegin(TransactionId tid)
        throws IOException {
        Debug.log("BEGIN");
        if(tidToFirstLogRecord.get(tid.getId()) != null){
            System.err.print("logXactionBegin: already began this tid\n");
            throw new IOException("double logXactionBegin()");
        }
        preAppend();
        raf.writeInt(BEGIN_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        tidToFirstLogRecord.put(tid.getId(), currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("BEGIN OFFSET = " + currentOffset);
    }

    /** Checkpoint the log and write a checkpoint record. */
    public void logCheckpoint() throws IOException {
        //make sure we have buffer pool lock before proceeding
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                //Debug.log("CHECKPOINT, offset = " + raf.getFilePointer());
                preAppend();
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet();
                Iterator<Long> els = keys.iterator();
                force();
                Database.getBufferPool().flushAllPages();
                startCpOffset = raf.getFilePointer();
                raf.writeInt(CHECKPOINT_RECORD);
                raf.writeLong(-1); //no tid , but leave space for convenience

                //write list of outstanding transactions
                raf.writeInt(keys.size());
                while (els.hasNext()) {
                    Long key = els.next();
                    Debug.log("WRITING CHECKPOINT TRANSACTION ID: " + key);
                    raf.writeLong(key);
                    //Debug.log("WRITING CHECKPOINT TRANSACTION OFFSET: " + tidToFirstLogRecord.get(key));
                    raf.writeLong(tidToFirstLogRecord.get(key));
                }

                //once the CP is written, make sure the CP location at the
                // beginning of the log file is updated
                endCpOffset = raf.getFilePointer();
                raf.seek(0);
                raf.writeLong(startCpOffset);
                raf.seek(endCpOffset);
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                //Debug.log("CP OFFSET = " + currentOffset);
            }
        }

        logTruncate();
    }

    /** Truncate any unneeded portion of the log to reduce its space
        consumption */
    public synchronized void logTruncate() throws IOException {
        preAppend();
        raf.seek(0);
        long cpLoc = raf.readLong();

        long minLogRecord = cpLoc;

        if (cpLoc != -1L) {
            raf.seek(cpLoc);
            int cpType = raf.readInt();
            @SuppressWarnings("unused")
            long cpTid = raf.readLong();

            if (cpType != CHECKPOINT_RECORD) {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }

            int numOutstanding = raf.readInt();

            for (int i = 0; i < numOutstanding; i++) {
                @SuppressWarnings("unused")
                long tid = raf.readLong();
                long firstLogRecord = raf.readLong();
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord;
                }
            }
        }

        // we can truncate everything before minLogRecord
        File newFile = new File("logtmp" + System.currentTimeMillis());
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
        logNew.seek(0);
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);

        raf.seek(minLogRecord);

        //have to rewrite log records since offsets are different after truncation
        while (true) {
            try {
                int type = raf.readInt();
                long record_tid = raf.readLong();
                long newStart = logNew.getFilePointer();

                Debug.log("NEW START = " + newStart);

                logNew.writeInt(type);
                logNew.writeLong(record_tid);

                switch (type) {
                case UPDATE_RECORD:
                    Page before = readPageData(raf);
                    Page after = readPageData(raf);

                    writePageData(logNew, before);
                    writePageData(logNew, after);
                    break;
                case CHECKPOINT_RECORD:
                    int numXactions = raf.readInt();
                    logNew.writeInt(numXactions);
                    while (numXactions-- > 0) {
                        long xid = raf.readLong();
                        long xoffset = raf.readLong();
                        logNew.writeLong(xid);
                        logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
                    }
                    break;
                case BEGIN_RECORD:
                    tidToFirstLogRecord.put(record_tid,newStart);
                    break;
                }

                //all xactions finish with a pointer
                logNew.writeLong(newStart);
                raf.readLong();

            } catch (EOFException e) {
                break;
            }
        }

        Debug.log("TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: " + (raf.length() - minLogRecord));

        raf.close();
        logFile.delete();
        newFile.renameTo(logFile);
        raf = new RandomAccessFile(logFile, "rw");
        raf.seek(raf.length());
        newFile.delete();

        currentOffset = raf.getFilePointer();
        //print();
    }

    /** 回滚指定的事务，将它更新的任何页面的状态设置为其更新前的状态。
     * 为了保留事务语义，不在已经提交的事务上调用它
    */
    public void rollback(TransactionId tid)
            throws NoSuchElementException, IOException {
        // bug
        try {
            synchronized (Database.getBufferPool()) {
                synchronized (this) {
                    preAppend();
//                    long firstRecordLog = logStartRecordMap.get(tid.getId());
//                    System.out.println("first record at:" + firstRecordLog + ", currentOffset at:" + currentOffset);

                    raf.seek(0);
                    List<Page> needRollBackPages = new ArrayList<>();

                    while (raf.getFilePointer() < raf.length()) {
                        int logType = raf.readInt();
                        if (logType == 1 || logType == 2 || logType == 4) {
                            long txid = raf.readLong();
                            long offset = raf.readLong();
                        } else if (logType == 3) {
                            // only  UPDATE_RECORD is needed
                            long txid = raf.readLong();
                            Page before = readPageData(raf);
                            Page after = readPageData(raf);
                            long offset = raf.readLong();

                            if (txid == tid.getId()) {
                                needRollBackPages.add(before);
                            }
                        } else if (logType == 5) {
                            long txid = raf.readLong();
                            int keys = raf.readInt();
                            for (int j = 0; j < keys; j++) {
                                raf.readLong();
                                raf.readLong();
                            }
                        }
                    }

                    //  roll back for page list
                    for (int i = needRollBackPages.size()-1; i >= 0; i--) {
                        Page targetPage = Database.getBufferPool().getPage(tid, needRollBackPages.get(i).getId(), Permissions.READ_WRITE);
//                        targetPage = needRollBackPages.get(i);
                        Page oldPage = needRollBackPages.get(i);
                        // bug
                        Database.getBufferPool().swapPageByNewValue(targetPage, oldPage);
                        oldPage.markDirty(true, tid);

                        // TODO 强制刷盘
                        DbFile heapFile = Database.getCatalog().getDatabaseFile(oldPage.getId().getTableId());
                        heapFile.writePage(oldPage);
                    }
                }
            }
        } catch (TransactionAbortedException e1) {
            e1.printStackTrace();
            rollback(tid);
        } catch (DbException e2) {
            e2.printStackTrace();
        } finally {
            raf.seek(raf.getFilePointer());
        }
    }

    /** Shutdown the logging system, writing out whatever state
        is necessary so that start up can happen quickly (without
        extensive recovery.)
    */
    public synchronized void shutdown() {
        try {
            logCheckpoint();  // way to shutdown is to write a checkpoint record
            raf.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    /** 通过确保安装已提交事务的更新和未安装未提交事务的更新来恢复数据库系统
    */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                recoveryUndecided = false;

                raf.seek(0);
                long checkPointOffset = raf.readLong();
                List<Long> undoTxIdList = new ArrayList<>();
                List<Long> redoAbortTxIdList = new ArrayList<>();
                List<Long> redoCommitTxIdList = new ArrayList<>();

                if (checkPointOffset > 0) {
                    raf.seek(checkPointOffset);

                    int logType = raf.readInt();
                    long checkPointid = raf.readLong();
                    int activeTransNum = raf.readInt();

                    for (int i = 0; i < activeTransNum; i++) {
                        long txId = raf.readLong();
                        long firstLogRecord = raf.readLong();
                        tidToFirstLogRecord.put(txId, firstLogRecord);
                        // init tid as undo
                        undoTxIdList.add(txId);
                    }
                }

                logScan(raf, undoTxIdList, redoAbortTxIdList, redoCommitTxIdList);

                // do undo
                for (int i = 0; i < undoTxIdList.size(); i++) {
                    doRecover(raf, undoTxIdList.get(i), true, true);
                }

                // do redo abort
                for (int i = 0; i < redoAbortTxIdList.size(); i++) {
                    doRecover(raf, redoAbortTxIdList.get(i), false, true);
                }

                for (int i = 0; i < redoCommitTxIdList.size(); i++) {
                    doRecover(raf, redoCommitTxIdList.get(i), false, false);
                }
            }
         }
    }

    class UpdatePagesHistory {
        private Page before;
        private Page after;

        public UpdatePagesHistory(Page before, Page after) {
            this.before = before;
            this.after = after;
        }

        public Page getBefore() {
            return before;
        }

        public void setBefore(Page before) {
            this.before = before;
        }

        public Page getAfter() {
            return after;
        }

        public void setAfter(Page after) {
            this.after = after;
        }
    }

    private void doRecover(RandomAccessFile raf, long targetTxId, boolean isUnDo, boolean isAbort) throws  IOException {
        raf.seek(0);

        List<UpdatePagesHistory> updateList = getUpdateListFromLog(raf, targetTxId);

        if(isAbort) {
            for (int i = updateList.size() -1; i >= 0; i--) {
                UpdatePagesHistory updatePageRecord = updateList.get(i);
                Page targetPage = updatePageRecord.getBefore();
                // flush before page
                DbFile heapFile = Database.getCatalog().getDatabaseFile(targetPage.getId().getTableId());
                heapFile.writePage(targetPage);

                if (isUnDo) {
                    // write abort to log
                    raf.writeInt(ABORT_RECORD);
                    raf.writeLong(targetTxId);
                    raf.writeLong(currentOffset);
                    currentOffset = raf.getFilePointer();
                }
            }
        } else {
            if (!isUnDo) {
                for (int i = 0; i < updateList.size(); i++) {
                    UpdatePagesHistory updatePageRecord = updateList.get(i);
                    Page targetPage = updatePageRecord.getAfter();
                    // flush after page
                    DbFile heapFile = Database.getCatalog().getDatabaseFile(targetPage.getId().getTableId());
                    heapFile.writePage(targetPage);
                }
            } else {
                System.out.println("Error happened");
                throw new IOException();
            }
        }
    }

    private List<UpdatePagesHistory> getUpdateListFromLog (RandomAccessFile raf, long targetTxId) throws IOException {
//        raf.seek(0);
        List<UpdatePagesHistory> updateList = new ArrayList<>();

        while (raf.getFilePointer() < raf.length()) {
            int logType = raf.readInt();

            if (logType == 1 || logType == 2 || logType == 4) { // 1 ABORT_RECORD  2 COMMIT_RECORD   4 BEGIN_TRANS
                long txid = raf.readLong();
                long offset = raf.readLong();
            } else if (logType == 3) {  // 3 UPDATE_RECORD
                // only UPDATE_RECORD is needed
                long txid = raf.readLong();
                Page before = readPageData(raf);
                Page after = readPageData(raf);
                long offset = raf.readLong();

                if (txid == targetTxId) {
                    UpdatePagesHistory pageHistory = new UpdatePagesHistory(before, after);
                    updateList.add(pageHistory);
                }
            } else if (logType == 5) {  // CHECKPOINT_RECORD
                long txid = raf.readLong();
                int keys = raf.readInt();
                for (int j = 0; j < keys; j++) {
                    raf.readLong();
                    raf.readLong();
                }
            }
        }

        return updateList;
    }


    private void logScan(RandomAccessFile raf, List<Long> undoTxIdList, List<Long> redoAbortTxIdList, List<Long> redoCommitTxIdList) throws IOException {
        while (raf.getFilePointer() < raf.length()) {
            int logType = raf.readInt();

            if (logType == 1) { // ABORT_RECORD
                long tid = raf.readLong();
                long offset = raf.readLong();

                // abort should redo
                undoTxIdList.remove(tid);
                redoAbortTxIdList.add(tid);
            } else if (logType == 2) { // COMMIT_RECORD
                long tid = raf.readLong();
                long offset = raf.readLong();

                // commit should redo
                undoTxIdList.remove(tid);
                redoCommitTxIdList.add(tid);
            } else if (logType == 3) { // UPDATE_RECORD
                long tid = raf.readLong();
                Page before = readPageData(raf);
                Page after = readPageData(raf);
                long offset = raf.readLong();
            } else if (logType == 4) { // BEGIN_TRANS
                long tid = raf.readLong();
                long offset = raf.readLong();

                // init as undo
                undoTxIdList.add(tid);
            }
        }
    }


    /** Print out a human readable represenation of the log */
    public void print() throws IOException {
        long curOffset = raf.getFilePointer();

        raf.seek(0);

        System.out.println("0: checkpoint record at offset " + raf.readLong());

        while (true) {
            try {
                int cpType = raf.readInt();
                long cpTid = raf.readLong();

                System.out.println((raf.getFilePointer() - (INT_SIZE + LONG_SIZE)) + ": RECORD TYPE " + cpType);
                System.out.println((raf.getFilePointer() - LONG_SIZE) + ": TID " + cpTid);

                switch (cpType) {
                case BEGIN_RECORD:
                    System.out.println(" (BEGIN)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;
                case ABORT_RECORD:
                    System.out.println(" (ABORT)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;
                case COMMIT_RECORD:
                    System.out.println(" (COMMIT)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;

                case CHECKPOINT_RECORD:
                    System.out.println(" (CHECKPOINT)");
                    int numTransactions = raf.readInt();
                    System.out.println((raf.getFilePointer() - INT_SIZE) + ": NUMBER OF OUTSTANDING RECORDS: " + numTransactions);

                    while (numTransactions-- > 0) {
                        long tid = raf.readLong();
                        long firstRecord = raf.readLong();
                        System.out.println((raf.getFilePointer() - (LONG_SIZE + LONG_SIZE)) + ": TID: " + tid);
                        System.out.println((raf.getFilePointer() - LONG_SIZE) + ": FIRST LOG RECORD: " + firstRecord);
                    }
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                    break;
                case UPDATE_RECORD:
                    System.out.println(" (UPDATE)");

                    long start = raf.getFilePointer();
                    Page before = readPageData(raf);

                    long middle = raf.getFilePointer();
                    Page after = readPageData(raf);

                    System.out.println(start + ": before image table id " + before.getId().getTableId());
                    System.out.println((start + INT_SIZE) + ": before image page number " + before.getId().getPageNumber());
                    System.out.println((start + INT_SIZE) + " TO " + (middle - INT_SIZE) + ": page data");

                    System.out.println(middle + ": after image table id " + after.getId().getTableId());
                    System.out.println((middle + INT_SIZE) + ": after image page number " + after.getId().getPageNumber());
                    System.out.println((middle + INT_SIZE) + " TO " + (raf.getFilePointer()) + ": page data");

                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                    break;
                }

            } catch (EOFException e) {
                //e.printStackTrace();
                break;
            }
        }

        // Return the file pointer to its original position
        raf.seek(curOffset);
    }

    public  synchronized void force() throws IOException {
        raf.getChannel().force(true);
    }

}
