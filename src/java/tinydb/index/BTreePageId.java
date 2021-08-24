package tinydb.index;

import tinydb.storage.BufferPool;
import tinydb.storage.PageId;

import java.util.Objects;

/** 唯一标识符 for BTreeInternalPage, BTreeLeafPage, BTreeHeaderPage
 *  and BTreeRootPtrPage objects. 
 */
public class BTreePageId implements PageId {

	public final static int ROOT_PTR = 0;
	public final static int INTERNAL = 1;
	public final static int LEAF = 2;
	public final static int HEADER = 3;

	private final int tableId;
	private final int pgNo;
	private final int pgcateg;

	static public String categToString(int categ) {
		switch (categ) {
			case ROOT_PTR:
				return "ROOT_PTR";
			case INTERNAL:
				return "INTERNAL";
			case LEAF:
				return "LEAF";
			case HEADER:
				return "HEADER";
			default:
				throw new IllegalArgumentException("categ");
		}
	}

	/**
	 * Create a page id structure for a specific page of a specific table.
	 *
	 * @param tableId The table that is being referenced
	 * @param pgNo The page number in that table.
	 * @param pgcateg which kind of page it is
	 */
	public BTreePageId(int tableId, int pgNo, int pgcateg) {
		this.tableId = tableId;
		this.pgNo = pgNo;
		this.pgcateg = pgcateg;
	}

	/** @return the table associated with this PageId */
	@Override
    public int getTableId() {
		return tableId;
	}

	/**
	 * @return the page number in the table getTableId() associated with
	 *   this PageId
	 */
	@Override
    public int getPageNumber() {
		return pgNo;
	}

	/**
	 * @return the category of this page
	 */
	public int pgcateg() {
		return pgcateg;
	}

	/**
	 * @return a hash code for this page, represented by the combination of
	 *   the table number, page number, and pgcateg
	 * @see BufferPool
	 */
	@Override
	public int hashCode() {
	    return Objects.hash(tableId, pgNo, pgcateg);
	}

	/**
	 * Compares one PageId to another.
	 * @param o The object to compare against (must be a PageId)
	 * @return true if the objects are equal
	 */
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof BTreePageId)) {
			return false;
		}
		BTreePageId p = (BTreePageId)o;
		return tableId == p.tableId && pgNo == p.pgNo && pgcateg == p.pgcateg;
	}

	@Override
	public String toString() {

        return "(tableId: " + tableId +
                ", pgNo: " + pgNo +
                ", pgcateg: " + categToString(pgcateg) +
                ")";
	}

	/**
	 *  Return a representation of this object as an array of integers, for writing to disk.
	 */
	@Override
    public int[] serialize() {
		int[] data = new int[3];

		data[0] = tableId;
		data[1] = pgNo;
		data[2] = pgcateg;

		return data;
	}

}
