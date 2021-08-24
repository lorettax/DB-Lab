package tinydb.storage;

import tinydb.common.Database;
import tinydb.common.DbException;
import tinydb.common.Catalog;
import tinydb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * HeapPage 的每个实例存储一页 HeapFiles 的数据并实现 BufferPool 使用的 Page 接口
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock= (byte) 0;

    // 将页面更改为脏的事务 ID
    private TransactionId dirtyId;
    // if the page is dirty
    private boolean dirty;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++) {
            header[i] = dis.readByte();
        }
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++) {
                tuples[i] = readNextTuple(dis,i);
            }
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }


    private int getNumTuples() {
        int num = (int)Math.floor((BufferPool.getPageSize()*8*1.0)/(td.getSize()*8+1));
        return num;
    }

    /**
     * 计算 HeapFile 中页面头部的字节数，每个元组占用 tupleSize 字节
     * */
    private int getHeaderSize() {

        return (int)Math.ceil(getNumTuples()*1.0/8);
                 
    }
    
    /** Return a view of this page before it was modified */
    @Override
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }
    
    @Override
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    @Override
    public HeapPageId getId() {
    return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }


    @Override
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty HeapPage.
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;
     * the corresponding header bit should be updated to reflect that it is no longer stored on any page.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {

        int tid = t.getRecordId().getTupleNumber();
        if(tuples[tid] == null){
            throw new DbException("tuple does not exist");
        }
        if(!isSlotUsed(tid)){
            throw new DbException("the slot is already empty");
        }
//        else if(!t.equals(tuples[tid])){
//            throw new DbException(String.format("tuple does not exits %d and %d",t.getRecordId().hashCode(),tuples[tid].getRecordId().hashCode()));
//        }
        else{
            markSlotUsed(tid,false);
            tuples[tid] = null;
        }
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {

        if(getNumEmptySlots() == 0 || !t.getTupleDesc().equals(td)){
            throw new DbException("page is full or tuple descriptor does not match");
        }
        for(int i=0;i<numSlots;++i){
            if(!isSlotUsed(i)){
                markSlotUsed(i,true);
                t.setRecordId(new RecordId(pid,i));
                tuples[i] = t;
                break;
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    @Override
    public void markDirty(boolean dirty, TransactionId tid) {

        this.dirty  = dirty;
        this.dirtyId = tid;

    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    @Override
    public TransactionId isDirty() {
        return this.dirty ? this.dirtyId:null;
    }

    /**
     * return 此 page 上的空槽(slot)数
     */
    public int getNumEmptySlots() {
        int cnt = 0;
        for(int i=0;i<numSlots;++i){
            if(!isSlotUsed(i)){
                ++cnt;
            }
        }
        return cnt;
    }

    /**
     * 如果此页面上的相关插槽已填充，则返回 true
     */
    public boolean isSlotUsed(int i) {
        int quot = i/8;
        int remainder = i%8;

        int bitidx = header[quot];
        int bit = (bitidx>>remainder) & 1;
        return bit == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        byte b = header[Math.floorDiv(i,8)];
        byte mask = (byte)(1<<(i%8));
        if(value){
            header[Math.floorDiv(i,8)] = (byte) (b|mask);
        }else{
            header[Math.floorDiv(i,8)] = (byte) (b&(~mask));
        }
    }

    public Iterator<Tuple> iterator() {
        ArrayList<Tuple> filledTuples = new ArrayList<Tuple>();
        for(int i=0;i<numSlots;++i){
            if(isSlotUsed(i)){
                filledTuples.add(tuples[i]);
            }
        }
        return filledTuples.iterator();
    }

}

