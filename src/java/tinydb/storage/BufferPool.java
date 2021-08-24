package tinydb.storage;

import tinydb.common.Database;
import tinydb.common.Permissions;
import tinydb.common.DbException;
import tinydb.transaction.TransactionAbortedException;
import tinydb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 检查 锁  获取页面
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
//    private final ConcurrentHashMap<Integer,Page> pageStore;

    private final ConcurrentHashMap<PageId,Page> pageStore;

    // transaction
    private int age;
    private final ConcurrentHashMap<PageId,Integer> pageAge;
    private PageLockManager lockManager;



    private class Lock{
        TransactionId tid;
        int lockType;   // 0 for shared lock and 1 for exclusive lock

        public Lock(TransactionId tid,int lockType){
            this.tid = tid;
            this.lockType = lockType;
        }
    }


    private class PageLockManager{
        ConcurrentHashMap<PageId,Vector<Lock>> lockMap;

        public PageLockManager(){
            lockMap = new ConcurrentHashMap<PageId,Vector<Lock>>();
        }

        public synchronized boolean acquireLock(PageId pid,TransactionId tid,int lockType){
            // if no lock held on pid
            if(lockMap.get(pid) == null){
                Lock lock = new Lock(tid,lockType);
                Vector<Lock> locks = new Vector<>();
                locks.add(lock);
                lockMap.put(pid,locks);

                return true;
            }

            // if some Tx holds lock on pid
            // locks.size() won't be 0 because releaseLock will remove 0 size locks from lockMap
            Vector<Lock> locks = lockMap.get(pid);

            // if tid already holds lock on pid
            for(Lock lock:locks){
                if(lock.tid == tid){
                    // already hold that lock
                    if(lock.lockType == lockType) {
                        return true;
                    }
                    // already hold exclusive lock when acquire shared lock
                    if(lock.lockType == 1) {
                        return true;
                    }
                    // already hold shared lock,upgrade to exclusive lock
                    if(locks.size()==1){
                        lock.lockType = 1;
                        return true;
                    }else{
                        return false;
                    }
                }
            }

            // if the lock is a exclusive lock
            if (locks.get(0).lockType ==1){
                assert locks.size() == 1 : "exclusive lock can't coexist with other locks";
                return false;
            }

            // if no exclusive lock is held, there could be multiple shared locks
            if(lockType == 0){
                Lock lock = new Lock(tid,0);
                locks.add(lock);
                lockMap.put(pid,locks);

                return true;
            }
            // can not acquire a exclusive lock when there are shard locks on pid
            return false;
        }


        public synchronized boolean releaseLock(PageId pid,TransactionId tid){
            // if not a single lock is held on pid
            System.out.println(lockMap.size());
            System.out.println("pid的 value: "+ pid);
            System.out.println("锁编号为： "+ lockMap.get(pid));
            assert lockMap.get(pid) != null : "page not locked!";
            Vector<Lock> locks = lockMap.get(pid);

            for(int i=0;i<locks.size();++i){
                Lock lock = locks.get(i);

                // release lock
                if(lock.tid == tid){
                    locks.remove(lock);

                    // if the last lock is released
                    // remove 0 size locks from lockMap
                    if(locks.size() == 0) {
                        lockMap.remove(pid);
                    }
                    return true;
                }
            }
            // not found tid in tids which lock on pid
            return false;
        }


        public synchronized boolean holdsLock(PageId pid,TransactionId tid){
            // if not a single lock is held on pid
            if(lockMap.get(pid) == null) {
                return false;
            }
            Vector<Lock> locks = lockMap.get(pid);

            // check if a tid exist in pid's vector of locks
            for(Lock lock:locks){
                if(lock.tid == tid){
                    return true;
                }
            }
            return false;
        }
    }



    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        pageStore = new ConcurrentHashMap<>();

        // transaction
        pageAge = new ConcurrentHashMap<PageId,Integer>();
        age = 0;
        lockManager = new PageLockManager();

    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

//        if(!pageStore.containsKey(pid.hashCode())){
//            DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
//            Page page = dbfile.readPage(pid);
//            pageStore.put(pid.hashCode(),page);
//        }
//        return pageStore.get(pid.hashCode());

//        if(!pageStore.containsKey(pid)){
//            if(pageStore.size()>numPages){
//                evictPage();
//            }
//            DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
//            Page page = dbfile.readPage(pid);
//            pageStore.put(pid,page);
//        }
//        return pageStore.get(pid);

        // trancaction
//        int lockType;
//        if(perm == Permissions.READ_ONLY){
//            lockType = 0;
//        }else{
//            lockType = 1;
//        }
//        boolean lockAcquired = false;
//
//        if(!pageStore.containsKey(pid)){
//            int tabId = pid.getTableId();
//            DbFile file = Database.getCatalog().getDatabaseFile(tabId);
//            Page page = file.readPage(pid);
//
//            if(pageStore.size()==numPages){
//                evictPage();
//            }
//            pageStore.put(pid,page);
//            pageAge.put(pid,age++);
//            return page;
//        }
//        return pageStore.get(pid);

        int lockType;
        if(perm == Permissions.READ_ONLY){
            lockType = 0;
        }else{
            lockType = 1;
        }
        boolean lockAcquired = false;
        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000) + 1000;
        while(!lockAcquired){
            long now = System.currentTimeMillis();
            if(now-start > timeout){
                // TransactionAbortedException means detect a deadlock
                // after upper caller catch TransactionAbortedException
                // will call transactionComplete to abort this transition
                // give someone else a chance: abort the transaction
                throw new TransactionAbortedException();
            }
            lockAcquired = lockManager.acquireLock(pid,tid,lockType);
        }

        if(!pageStore.containsKey(pid)){
            int tabId = pid.getTableId();
            DbFile file = Database.getCatalog().getDatabaseFile(tabId);
            Page page = file.readPage(pid);

            if(pageStore.size()==numPages){
                evictPage();
            }
            pageStore.put(pid,page);
            pageAge.put(pid,age++);
            return page;
        }
        return pageStore.get(pid);

    }

    public synchronized void swapPageByNewValue(Page targetPage, Page newPage) {

        // bug
        for(Map.Entry<PageId,Page> entry: pageStore.entrySet()) {
//            System.out.println("entry.getKey: " +entry.getKey() +" entry.getValue: "+ entry.getValue());
//            System.out.println(entry.getValue().getPageData());
//            System.out.println(newPage.getPageData());
            if(entry.getValue().equals(targetPage)) {

//                System.out.println("找到目标"+ newPage.getId() + "  要替换成的页：" + newPage);
                pageStore.put(newPage.getId(), newPage);
            }
        }

//        Page page = pageStore.get(pageId);
//        if (page.getId().equals(pageId)) {
//            pageStore.put(pageId, newPage);
//        }
//        for (int i = 0; i < pageStore.size(); i++) {
//            if (pageStore.get(i).getId().equals(pageId)) {
//                pageStore.put(pageId, newPage);
//            }
//        }
    }


    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {

        lockManager.releaseLock(pid,tid);
    }

    /**
     * 释放与给定事务关联的所有锁
     */
    public void transactionComplete(TransactionId tid) throws IOException {

        transactionComplete(tid,true);
    }

    /** 如果指定事务在指定页面上有锁，则返回 true */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(p,tid);
    }


    private synchronized void restorePages(TransactionId tid) {

        for (PageId pid : pageStore.keySet()) {
            Page page = pageStore.get(pid);

            if (page.isDirty() == tid) {
                int tabId = pid.getTableId();
                DbFile file =  Database.getCatalog().getDatabaseFile(tabId);
                Page pageFromDisk = file.readPage(pid);

                pageStore.put(pid, pageFromDisk);
            }
        }
    }


    /**
     * 提交或中止给定的事务；释放与事务关联的所有锁
     */
    public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
        if(commit){
            flushPages(tid);
        }else{
            restorePages(tid);
        }

        for(PageId pid:pageStore.keySet()){
            if(holdsLock(tid,pid)) {
                unsafeReleasePage(tid,pid);
            }
        }

    }

    /**
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        updateBufferPool((ArrayList<Page>) f.insertTuple(tid,t),tid);
    }

    /**
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        updateBufferPool((ArrayList<Page>) f.deleteTuple(tid,t),tid);
    }


    private void updateBufferPool(ArrayList<Page> pagelist, TransactionId tid) throws DbException{
//        for(Page p:pagelist){
//            p.markDirty(true,tid);
//            // update bufferpool
//            if(pageStore.size() > numPages) {
//                evictPage();
//            }
//            pageStore.put(p.getId().hashCode(),p);
//        }

        for(Page p:pagelist){
            p.markDirty(true,tid);
            // update bufferpool
            if(pageStore.size() > numPages) {
                evictPage();
            }
            pageStore.put(p.getId(),p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     */
    public synchronized void flushAllPages() throws IOException {
        for(Page p:pageStore.values()){
            flushPage(p.getId());
        }
    }


    public synchronized void discardPage(PageId pid) {

        pageStore.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {

        Page p = pageStore.get(pid);
        TransactionId tid = null;
        // flush it if it is dirty
        if((tid = p.isDirty())!= null){
            Database.getLogFile().logWrite(tid,p.getBeforeImage(),p);
            Database.getLogFile().force();
            // write to disk
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
            p.markDirty(false,null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {

        for (PageId pid : pageStore.keySet()) {
            Page page = pageStore.get(pid);
            if (page.isDirty() == tid) {
                flushPage(pid);
            }
        }
    }

    /**
     * 从缓冲池中丢弃一个页面。将页面刷新到磁盘以确保脏页面在磁盘上更新
     */
    private synchronized  void evictPage() throws DbException {
//        PageId pid = new ArrayList<>(pageStore.keySet()).get(0);
//        try{
//            flushPage(pid);
//        }catch(IOException e){
//            e.printStackTrace();
//        }
//        discardPage(pid);

        // transaction

        assert numPages == pageStore.size() : "Buffor Pool is not full, not need to evict page";

        PageId pageId = null;
        int oldestAge = -1;

        // find the oldest page to evict (which is not dirty)
        for (PageId pid: pageAge.keySet()) {
            Page page = pageStore.get(pid);
            // skip dirty page
            if (page.isDirty() != null) {
                continue;
            }

            if (pageId == null) {
                pageId = pid;
                oldestAge = pageAge.get(pid);
                continue;
            }

            if (pageAge.get(pid) < oldestAge) {
                pageId = pid;
                oldestAge = pageAge.get(pid);
            }
        }

        if (pageId == null) {
            throw  new DbException("failed to evict page: all pages are either dirty");
        }
        Page page = pageStore.get(pageId);

        // evict page
        pageStore.remove(pageId);
        pageAge.remove(pageId);

    }

}
