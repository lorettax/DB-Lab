package tinydb.index;

import tinydb.storage.Field;
import tinydb.storage.RecordId;

import java.io.Serializable;

/**
 * Each instance of BTreeEntry stores one key and two child page ids.
 */
public class BTreeEntry implements Serializable {

	private static final long serialVersionUID = 1L;
	
	/** The key of this entry * */
	private Field key;

	/**
	 * The left child page id
	 * */
	private BTreePageId leftChild;

	/**
	 * The right child page id
	 * */
	private BTreePageId rightChild;

	/**
	 * The record id of this entry
	 * */
	private RecordId rid; // null if not stored on any page

	/**
	 * Constructor to create a new BTreeEntry
	 * @param key - the key
	 * @param leftChild - page id of the left child
	 * @param rightChild - page id of the right child
	 */
	public BTreeEntry(Field key, BTreePageId leftChild, BTreePageId rightChild) {
		this.key = key;
		this.leftChild = leftChild;
		this.rightChild = rightChild;
	}
	
	/**
	 * @return the key
	 */
	public Field getKey() {
		return key;
	}
	
	/**
	 * @return the left child page id
	 */
	public BTreePageId getLeftChild() {
		return leftChild;
	}
	
	/**
	 * @return the right child page id
	 */
	public BTreePageId getRightChild() {
		return rightChild;
	}
	
	/**
	 * @return the record id of this entry, representing the location of this entry
	 * in a BTreeFile. May be null if this entry is not stored on any page in the file
	 */
	public RecordId getRecordId() {
		return rid;
	}
	
	/**
	 * Set the key for this entry.
	 */
	public void setKey(Field key) {
		this.key = key;
	}
	
	/**
	 * Set the left child id for this entry.
	 * @param leftChild - the new left child
	 */
	public void setLeftChild(BTreePageId leftChild) {
		this.leftChild = leftChild;
	}
	
	/**
	 * Set the right child id for this entry.
	 * @param rightChild - the new right child
	 */
	public void setRightChild(BTreePageId rightChild) {
		this.rightChild = rightChild;
	}
	
	/**
	 * set the record id for this entry
	 * @param rid - the new record id
	 */
	public void setRecordId(RecordId rid) {
		this.rid = rid;
	}
	
	/**
	 * Prints a representation of this BTreeEntry
	 */
	@Override
	public String toString() {
		return "[" + leftChild.getPageNumber() + "|" + key + "|" + rightChild.getPageNumber() + "]";
	}
	
}

