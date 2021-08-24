package tinydb.storage;

import java.io.Serializable;

/**
 * RecordId 是对特定 table 的特定 page 上的特定 tuple 的引用
 */
public class RecordId implements Serializable {


    private static final long serialVersionUID = 1L;

    private final PageId pid;
    private final int tupleno;

    /**
     * 创建一个新的 RecordId 引用指定的 PageId 和 tuple 编号。
     * 
     * @param pid 元组所在页面的 pageid
     * @param tupleno
     *            page 中的 tuple 编号
     */
    public RecordId(PageId pid, int tupleno) {
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return 此 RecordId 引用的 tuple 编号。
     */
    public int getTupleNumber() {
        return tupleno;
    }

    /**
     * @return 此 RecordId 引用的 page ID
     */
    public PageId getPageId() {
        return pid;
    }

    /**
     * 如果两个 RecordId 对象表示相同的 tuple, 则认为它们相等
     * 
     * @return 如果 this 和 o 代表同一个元组，则为 true
     */
    @Override
    public boolean equals(Object o) {
        if(o instanceof RecordId){
            RecordId ro = (RecordId) o;
            if(ro.getPageId().equals(pid) && ro.getTupleNumber() == tupleno){
                return true;
            }
        }
        return false;

    }

    /**
     * @return 对于相等的 RecordId 对象是相同的 int.
     */
    @Override
    public int hashCode() {
        String hash = "" + pid.getTableId()+pid.getPageNumber() + tupleno;
        return hash.hashCode();
    }

}
