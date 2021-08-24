package tinydb.storage;

import tinydb.common.Database;
import tinydb.common.DbException;
import tinydb.common.Permissions;
import tinydb.transaction.TransactionAbortedException;
import tinydb.transaction.TransactionId;

import java.io.*;
import java.util.*;


public class HeapFile implements DbFile {


    private final File file;
    private final TupleDesc td;


    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
    }


    public File getFile() {
        return file;
    }


    @Override
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }


    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public Page readPage(PageId pid) {
        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();

        RandomAccessFile f = null;
        try{
            f = new RandomAccessFile(file,"r");
            if((pgNo+1)*BufferPool.getPageSize() > f.length()){
                f.close();
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
            }
            byte[] bytes = new byte[BufferPool.getPageSize()];
            f.seek(pgNo * BufferPool.getPageSize());

            int read = f.read(bytes,0,BufferPool.getPageSize());
            if(read != BufferPool.getPageSize()){
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableId, pgNo, read));
            }
            HeapPageId id = new HeapPageId(pid.getTableId(),pid.getPageNumber());
            return new HeapPage(id,bytes);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try{
                f.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
   }


    @Override
    public void writePage(Page page) throws IOException {

        int pgNo = page.getId().getPageNumber();
        if(pgNo > numPages()){
            throw new IllegalArgumentException();
        }
        int pgSize = BufferPool.getPageSize();
        //write IO
        RandomAccessFile f = new RandomAccessFile(file,"rw");
        // set offset
        f.seek(pgNo*pgSize);
        // write
        byte[] data = page.getPageData();
        f.write(data);
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int num = (int)Math.floor(file.length()*1.0/BufferPool.getPageSize());
        return num;
    }

    // see DbFile.java for javadocs
    @Override
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
//        ArrayList<Page> pageList= new ArrayList<Page>();
//        for(int i=0;i<numPages();++i){
//            // took care of getting new page
//            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
//                    new HeapPageId(this.getId(),i),Permissions.READ_WRITE);
//            if(p.getNumEmptySlots() == 0) {
//                continue;
//            }
//            p.insertTuple(t);
//            pageList.add(p);
//            return pageList;
//        }
//        // no new page
//        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file,true));
//        byte[] emptyData = HeapPage.createEmptyPageData();
//        bw.write(emptyData);
//        bw.close();
//        // load into cache
//        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
//                new HeapPageId(getId(),numPages()-1),Permissions.READ_WRITE);
//        p.insertTuple(t);
//        pageList.add(p);
//        return pageList;


        HeapPage page  = null;

        // find a non full page
        for(int i=0;i<numPages();++i){
            HeapPageId pid = new HeapPageId(getId(),i);
            page = (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
            if(page.getNumEmptySlots()!=0){
                break;
            } else{
                Database.getBufferPool().unsafeReleasePage(tid,pid);
            }
        }

        // if not exist an empty slot, create a new page to store
        if(page == null || page.getNumEmptySlots() == 0){
            HeapPageId pid = new HeapPageId(getId(),numPages());
            byte[] data = HeapPage.createEmptyPageData();
            HeapPage heapPage = new HeapPage(pid,data);
            writePage(heapPage);
            page = (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        }

        page.insertTuple(t);

        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;
    }

    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {

//        ArrayList<Page> pageList = new ArrayList<Page>();
//        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
//                t.getRecordId().getPageId(),Permissions.READ_WRITE);
//        p.deleteTuple(t);
//        pageList.add(p);
//        return pageList;

        // transaction
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();

        // delete tuple and mark page as dirty
        HeapPage page =  (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        page.deleteTuple(t);

        // return res
        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;

    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this,tid);
    }


    private static final class HeapFileIterator implements DbFileIterator{
        private final HeapFile heapFile;
        private final TransactionId tid;
        private Iterator<Tuple> it;
        private int whichPage;

        public HeapFileIterator(HeapFile file,TransactionId tid){
            this.heapFile = file;
            this.tid = tid;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            whichPage = 0;
            it = getPageTuples(whichPage);
        }

        private Iterator<Tuple> getPageTuples(int pageNumber) throws TransactionAbortedException, DbException{
            if(pageNumber >= 0 && pageNumber < heapFile.numPages()){
                HeapPageId pid = new HeapPageId(heapFile.getId(),pageNumber);
                HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }else{
                throw new DbException(String.format("heapfile %d does not contain page %d!", pageNumber,heapFile.getId()));
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            if(it == null){
                return false;
            }

            if(!it.hasNext()){
                if(whichPage < (heapFile.numPages()-1)){
                    whichPage++;
                    it = getPageTuples(whichPage);
                    return it.hasNext();
                }else{
                    return false;
                }
            }else{
                return true;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // TODO Auto-generated method stub
            if(it == null || !it.hasNext()){
                throw new NoSuchElementException();
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            close();
            open();
        }

        @Override
        public void close() {
            // TODO Auto-generated method stub
            it = null;
        }

    }
}

