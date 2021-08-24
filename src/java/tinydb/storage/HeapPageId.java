package tinydb.storage;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    private final int tableId;
    private final int pgNo;
    /**
     * Create a page id structure for a specific page of a specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        this.tableId = tableId;
        this.pgNo = pgNo;

    }

    /** @return the table associated with this PageId */
    @Override
    public int getTableId() {
        return this.tableId;
    }

    /**
     * @return
     * 与此 PageId 关联的表 getTableId() 中的 page number
     */
    @Override
    public int getPageNumber() {
        // some code goes here
        return this.pgNo;
    }

    /**
     * @return a hash code for this page, represented by a combination of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    @Override
    public int hashCode() {
        String hash = "" + tableId +pgNo;
        return hash.hashCode();

//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     */
    @Override
    public boolean equals(Object o) {
        if(o instanceof PageId){
            PageId pi = (PageId) o;
            if(pi.getTableId() == tableId && pi.getPageNumber() == pgNo){
                return true;
            }
        }
        return false;
    }

    @Override
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
