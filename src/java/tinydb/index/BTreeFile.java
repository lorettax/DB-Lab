package tinydb.index;

import java.io.*;
import java.util.*;

import tinydb.common.Database;
import tinydb.common.Permissions;
import tinydb.execution.IndexPredicate;
import tinydb.execution.Predicate.Op;
import tinydb.common.DbException;
import tinydb.common.Debug;
import tinydb.index.*;
import tinydb.storage.*;
import tinydb.transaction.TransactionAbortedException;
import tinydb.transaction.TransactionId;

/**
 * BTreeFile 是存储 B+ 树的 DbFile 的实现
 具体来说，它存储一个指向根页面、一组内部页面和一组叶页面的指针，这些页面包含一组按排序顺序的元组。
 BTreeFile 与 BTreeLeafPage、BTreeInternalPage 和 BTreeRootPtrPage 密切配合
 这些页面的格式在它们的构造函数中描述
 *
 * @see BTreeLeafPage#BTreeLeafPage
 * @see BTreeInternalPage#BTreeInternalPage
 * @see BTreeHeaderPage#BTreeHeaderPage
 * @see BTreeRootPtrPage#BTreeRootPtrPage
 * @author Becca Taft
 */
public class BTreeFile implements DbFile {

	private final File f;
	private final TupleDesc td;
	private final int tableid ;
	private final int keyField;

	/**
	 * Constructs a B+ tree file backed by the specified file.
	 *
	 * @param f - the file that stores the on-disk backing store for this B+ tree file.
	 * @param key - the field which index is keyed on
	 * @param td - the tuple descriptor of tuples in the file
	 */
	public BTreeFile(File f, int key, TupleDesc td) {
		this.f = f;
		this.tableid = f.getAbsoluteFile().hashCode();
		this.keyField = key;
		this.td = td;
	}

	/**
	 * Returns the File backing this BTreeFile on disk.
	 */
	public File getFile() {
		return f;
	}

	/**
	 *
	 返回唯一标识此 BTreeFile 的 ID。实施注意事项：、
	 您需要在某处生成此 tableid 并确保每个 BTreeFile 都有一个“唯一 ID”，并且您始终为特定的 BTreeFile 返回相同的值。
	 我们建议散列 BTreeFile 底层文件的绝对文件名，即 f.getAbsoluteFile().hashCode()
	 *
	 * @return an ID uniquely identifying this BTreeFile.
	 */
	@Override
	public int getId() {
		return tableid;
	}

	/**
	 *
	 * 返回存储在这个DbFile中的表的TupleDesc
	 *
	 * @return TupleDesc of this DbFile.
	 */
	@Override
	public TupleDesc getTupleDesc() {
		return td;
	}

	/**
	 *
	 * 从磁盘上的文件中读取一页。这不应直接调用，而应通过 getPage() 从 BufferPool 调用
	 *
	 * @param pid -要从磁盘读取的页面的 id
	 * @return  从磁盘上的内容构建的页面
	 */
	@Override
	public Page readPage(PageId pid) {
		BTreePageId id = (BTreePageId) pid;

		try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f))) {
			if (id.pgcateg() == BTreePageId.ROOT_PTR) {
				byte[] pageBuf = new byte[BTreeRootPtrPage.getPageSize()];
				int retval = bis.read(pageBuf, 0, BTreeRootPtrPage.getPageSize());
				if (retval == -1) {
					throw new IllegalArgumentException("Read past end of table");
				}
				if (retval < BTreeRootPtrPage.getPageSize()) {
					throw new IllegalArgumentException("Unable to read "
							+ BTreeRootPtrPage.getPageSize() + " bytes from BTreeFile");
				}
				Debug.log(1, "BTreeFile.readPage: read page %d", id.getPageNumber());
				return new BTreeRootPtrPage(id, pageBuf);
			} else {
				byte[] pageBuf = new byte[BufferPool.getPageSize()];
				if (bis.skip(BTreeRootPtrPage.getPageSize() + (long) (id.getPageNumber() - 1) * BufferPool.getPageSize()) !=
						BTreeRootPtrPage.getPageSize() + (long) (id.getPageNumber() - 1) * BufferPool.getPageSize()) {
					throw new IllegalArgumentException(
							"Unable to seek to correct place in BTreeFile");
				}
				int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
				if (retval == -1) {
					throw new IllegalArgumentException("Read past end of table");
				}
				if (retval < BufferPool.getPageSize()) {
					throw new IllegalArgumentException("Unable to read "
							+ BufferPool.getPageSize() + " bytes from BTreeFile");
				}
				Debug.log(1, "BTreeFile.readPage: read page %d", id.getPageNumber());
				if (id.pgcateg() == BTreePageId.INTERNAL) {
					return new BTreeInternalPage(id, pageBuf, keyField);
				} else if (id.pgcateg() == BTreePageId.LEAF) {
					return new BTreeLeafPage(id, pageBuf, keyField);
				} else { // id.pgcateg() == BTreePageId.HEADER
					return new BTreeHeaderPage(id, pageBuf);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		// Close the file on success or error
		// Ignore failures closing the file
	}

	/**
	 *
	 * 将页写入磁盘。这不应该直接调用，但应该在将页刷新到磁盘时从缓冲池调用
	 *
	 * @param page - the page to write to disk
	 */
	@Override
	public void writePage(Page page) throws IOException {

		BTreePageId id = (BTreePageId) page.getId();

		byte[] data = page.getPageData();

		RandomAccessFile rf = new RandomAccessFile(f, "rw");

		if(id.pgcateg() == BTreePageId.ROOT_PTR) {
			rf.write(data);
			rf.close();
		} else {
			rf.seek(BTreeRootPtrPage.getPageSize() + (long) (page.getId().getPageNumber() - 1) * BufferPool.getPageSize());
			rf.write(data);
			rf.close();
		}
	}

	/**
	 * Returns the number of pages in this BTreeFile. 返回这个BTreeFile中的页数。
	 */
	public int numPages() {
		// we only ever write full pages
		return (int) ((f.length() - BTreeRootPtrPage.getPageSize())/ BufferPool.getPageSize());
	}

	/**
	 * Returns the index of the field that this B+ tree is keyed on 返回这个B+树的键值所在字段的索引
	 */
	public int keyField() {
		return keyField;
	}

	/**
	 *
	 * 递归函数，它在 B+ 树中找到并锁定与最左边可能包含关键字段 f 的页面相对应的叶页面。它以 READ_ONLY 权限锁定叶节点路径上的所有内部节点，并以权限 perm 锁定叶节点
	 *
	 *如果 f 为空，它会找到最左边的叶子页面——用于迭代器
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pid - the current page being searched
	 * @param perm - the permissions with which to lock the leaf page
	 * @param f - the field to search for
	 * @return the left-most leaf page possibly containing the key field f
	 *
	 */
	private BTreeLeafPage findLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, Permissions perm, Field f)
			throws DbException, TransactionAbortedException {
		System.out.println("start search @ B+Tree, pid = " + pid);


		if (pid.pgcateg() == BTreePageId.LEAF ) {
			// is leaf page and add self Perm
			Page targetPage = getPage(tid, dirtypages, pid, perm);
			System.out.println("get B+Tree Leaf Node\n");
			return (BTreeLeafPage) targetPage;
		} else if (pid.pgcateg() == BTreePageId.INTERNAL) {
			// in index
			Page targetPage = getPage(tid, dirtypages, pid, Permissions.READ_ONLY);
			Iterator<BTreeEntry> iterator = ((BTreeInternalPage)targetPage).iterator();
			BTreePageId targetPageId = null;
			BTreeEntry bTreeLeafPageEntry = null;

			boolean hasGreatThenField = false;
			// try find
			while (iterator.hasNext()) {
				bTreeLeafPageEntry = iterator.next();
				Field currentField = bTreeLeafPageEntry.getKey();

				if (f == null || f.compare(Op.LESS_THAN_OR_EQ, currentField)) {
					targetPageId = bTreeLeafPageEntry.getLeftChild();
					hasGreatThenField = true;
					break;
				}
			}

			// get the right one 得到正确的
			if (!hasGreatThenField) {
				targetPageId = bTreeLeafPageEntry.getRightChild();
			}

			return findLeafPage( tid,  dirtypages,  targetPageId,  perm, f);
		}

		return null;
	}


	/**
	 * 在没有dirtypages HashMap时查找叶页的方便方法
	 * 由 BTreeFile 迭代器使用。
	 * @see #findLeafPage(TransactionId, Map, BTreePageId, Permissions, Field)
	 *
	 * @param tid - the transaction id
	 * @param pid - the current page being searched
	 * @param f - the field to search for
	 * @return the left-most leaf page possibly containing the key field f
	 *
	 */
	BTreeLeafPage findLeafPage(TransactionId tid, BTreePageId pid, Field f)
			throws DbException, TransactionAbortedException {
		return findLeafPage(tid, new HashMap<>(), pid, Permissions.READ_ONLY, f);
	}

	/**
	 * 拆分叶页以为新元组腾出空间，并递归拆分父节点根据需要来容纳新条目。
	 * 新条目应该有一个与关键字字段匹配的关键字右边页面的第一个元组(键被“复制”)和子指针
	 * 指向由拆分产生的两个叶页。根据需要更新同级指针和父指针。
	 *
	 * Return the leaf page into which a new tuple with key field "field" should be inserted.
	 * 返回一个叶子页，其中应该插入一个具有关键字段“field”的新元组
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the leaf page to split
	 * @param field - 分割完成后要插入的元组的关键字段。有必要知道
	 * which of the two pages to return.
	 * @see #getParentWithEmptySlots(TransactionId, Map, BTreePageId, Field)
	 *
	 * @return the leaf page into which the new tuple should be inserted
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public BTreeLeafPage splitLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeLeafPage page, Field field)
			throws DbException, IOException, TransactionAbortedException {

		// 通过在现有页面的右侧添加一个新页面并将一半元组移动到新页面来拆分叶页面
		// 将中间键向上复制到父页面中，并根据需要递归拆分父页面以容纳新条目
		// getParentWithEmtpySlots() 在这里很有用 更新
		// 所有受影响的叶子页面的兄弟指针。返回应插入具有给定键字段的元组的页面


		BTreeLeafPage newLeftLeafPage = (BTreeLeafPage)getEmptyPage(tid, dirtypages, BTreePageId.LEAF);
		BTreeLeafPage newRightLeafPage = (BTreeLeafPage)getEmptyPage(tid, dirtypages, BTreePageId.LEAF);
		int middleIndex = ( page.getMaxTuples() + 1 ) / 2;
		Tuple splitTuple = null;

		// start Data copy
		Iterator<Tuple> fullPageIterator = page.iterator();
		for (int i = 0; fullPageIterator.hasNext(); i++) {
			Tuple targetTuple = fullPageIterator.next();
			if (field.compare(Op.LESS_THAN_OR_EQ, targetTuple.getField(0))) {
				Tuple insertTuple = new Tuple( targetTuple.getTupleDesc() );
				insertTuple.setField(0, field);
				i++;
				newLeftLeafPage.insertTuple(insertTuple);
			}

			newLeftLeafPage.insertTuple(targetTuple);

			if (i == middleIndex) {
				splitTuple = targetTuple;
				break;
			}
		}
		while (fullPageIterator.hasNext()) {
			Tuple targetTuple = fullPageIterator.next();
			if (field.compare(Op.LESS_THAN_OR_EQ, targetTuple.getField(0))) {
				Tuple insertTuple = new Tuple( targetTuple.getTupleDesc() );
				insertTuple.setField(0, field);
				newRightLeafPage.insertTuple(insertTuple);
			}

			newRightLeafPage.insertTuple(targetTuple);
		}
		System.out.println("newLeftLeafPage tuple count : " + newLeftLeafPage.getNumTuples()
				+ ", newRightLeafPage tuple count:" + newRightLeafPage.getNumTuples());
		// finished data copy
		return null;

	}

	/**
	 * 拆分内部页面为新条目腾出空间，并根据需要递归拆分其父页面以容纳新条目。
	 * 父项的新条目应该有一个与被拆分的原始内部页面中的中间键匹配的键（这个键被“推到”父级）。
	 * 新父条目的子指针应指向拆分后的两个内部页面。根据需要更新父指针。
	 *
	 * 返回应插入带有关键字段“field”的条目的内部页面
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page to split
	 * @param field - the key field of the entry to be inserted after the split is complete. Necessary to know which of the two pages to return.
	 * @see #getParentWithEmptySlots(TransactionId, Map, BTreePageId, Field)
	 * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
	 *
	 * @return the internal page into which the new entry should be inserted
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public BTreeInternalPage splitInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
											   BTreeInternalPage page, Field field)
			throws DbException, IOException, TransactionAbortedException {


		// 通过在现有页面的右侧添加一个新页面并将一半的条目移动到新页面来拆分内部页面。
		// 将中间键向上推入父页面，并根据需要递归拆分父页面以容纳新条目。
		// getParentWithEmtpySlots() 在这里很有用。
		// 更新所有移动到新页面的孩子的父指针。
		// updateParentPointers() 在这里很有用


		BTreeInternalPage newLeafInternalPage = (BTreeInternalPage)getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);
		BTreeInternalPage newRightInternalPage = (BTreeInternalPage)getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);
		int middleIndex = (page.getMaxEntries() + 1) / 2;
		BTreeEntry splitEntry = null;

		// start copy data
		Iterator<BTreeEntry> fullPageIterator = page.iterator();
		for (int i = 0; fullPageIterator.hasNext(); i++) {
			BTreeEntry targetEntry = fullPageIterator.next();
			if (field.compare(Op.LESS_THAN_OR_EQ, targetEntry.getKey())) {
				BTreeEntry constr_Entry = new BTreeEntry(field, targetEntry.getLeftChild(), targetEntry.getRightChild());
				newLeafInternalPage.insertEntry(constr_Entry);
			}
			newLeafInternalPage.insertEntry(targetEntry);

			if (i == middleIndex) {
				splitEntry = targetEntry;
				break;
			}
		}
		while (fullPageIterator.hasNext()) {
			BTreeEntry targetEntry = fullPageIterator.next();
			if (field.compare(Op.LESS_THAN_OR_EQ, targetEntry.getKey())) {
				BTreeEntry constr_Entry = new BTreeEntry(field, targetEntry.getLeftChild(), targetEntry.getRightChild());
				newRightInternalPage.insertEntry(constr_Entry);
			}
			newRightInternalPage.insertEntry(targetEntry);
		}
		System.out.println("newLeafInternalPage Entry count : "+ newRightInternalPage.getNumEntries()
				+ ", newRightInternalPage entry count : " + newRightInternalPage.getNumEntries());

		return null;
	}

	/**
	 *	 方法封装使父页面准备好接受新条目的过程。
	 * 这可能意味着创建一个页面来成为树的新根，分解现有的根父页如果没有空槽，或者简单地锁定并返回现有的父页。
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param parentId - the id of the parent. May be an internal page or the RootPtr page
	 * @param field - the key of the entry which will be inserted. Needed in case the parent must be split
	 * to accommodate the new entry
	 * @return the parent page, guaranteed to have at least one empty slot
	 * @see #splitInternalPage(TransactionId, Map, BTreeInternalPage, Field)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private BTreeInternalPage getParentWithEmptySlots(TransactionId tid, Map<PageId, Page> dirtypages,
													  BTreePageId parentId, Field field) throws DbException, IOException, TransactionAbortedException {

		BTreeInternalPage parent = null;

		// 如有必要，创建一个父节点
		// 这将是树的新根
		if(parentId.pgcateg() == BTreePageId.ROOT_PTR) {
			parent = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);

			// update the root pointer
			BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages,
					BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
			BTreePageId prevRootId = rootPtr.getRootId(); //save prev id before overwriting.
			rootPtr.setRootId(parent.getId());

			// update the previous root to now point to this new root.
			BTreePage prevRootPage = (BTreePage)getPage(tid, dirtypages, prevRootId, Permissions.READ_WRITE);
			prevRootPage.setParentId(parent.getId());
		}
		else {
			// lock the parent page
			parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId,
					Permissions.READ_WRITE);
		}

		// split the parent if needed
		if(parent.getNumEmptySlots() == 0) {
			parent = splitInternalPage(tid, dirtypages, parent, field);
		}

		return parent;

	}

	/**
	 * 帮助函数更新节点的父指针。
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pid - id of the parent node
	 * @param child - id of the child node to be updated with the parent pointer
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	private void updateParentPointer(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, BTreePageId child)
			throws DbException, TransactionAbortedException {

		BTreePage p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_ONLY);

		if(!p.getParentId().equals(pid)) {
			p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_WRITE);
			p.setParentId(pid);
		}

	}

	/**
	 * Update the parent pointer of every child of the given page so that it correctly points to the parent
	 * 更新给定页面的每个子级的父指针，使其正确指向父级
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the parent page
	 * @see #updateParentPointer(TransactionId, Map, BTreePageId, BTreePageId)
	 *
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	private void updateParentPointers(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage page)
			throws DbException, TransactionAbortedException{
		Iterator<BTreeEntry> it = page.iterator();
		BTreePageId pid = page.getId();
		BTreeEntry e = null;
		while(it.hasNext()) {
			e = it.next();
			updateParentPointer(tid, dirtypages, pid, e.getLeftChild());
		}
		if(e != null) {
			updateParentPointer(tid, dirtypages, pid, e.getRightChild());
		}
	}

	/**
	 * 封装锁定/获取页面过程的方法。首先该方法检查本地缓存（“dirtypages”），
	 * 如果在那里找不到请求的页面，它会从缓冲池中获取它。如果使用读写权限获取页面，它还会将页面添加到脏页缓存中，因为据推测它们很快就会被此事务弄脏
	 *
	 * 如果多次访问相同的页面，则需要此方法以确保页面更新不会丢失
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pid - the id of the requested page
	 * @param perm - the requested permissions on the page
	 * @return the requested page
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	Page getPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, Permissions perm)
			throws DbException, TransactionAbortedException {

		if (dirtypages.containsKey(pid)) {
			return dirtypages.get(pid);
		} else {
			Page p = Database.getBufferPool().getPage(tid, pid, perm);
			if (perm == Permissions.READ_WRITE) {
				dirtypages.put(pid, p);
			}
			return p;
		}

	}

	/**
	 * Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
	 May cause pages to split if the page where tuple t belongs is full.
	 *
	 * @param tid - the transaction id
	 * @param t - the tuple to insert
	 * @return a list of all pages that were dirtied by this operation. Could include
	many pages since parent pointers will need to be updated when an internal node splits.
	 * @see #splitLeafPage(TransactionId, Map, BTreeLeafPage, Field)
	 */
	@Override
	public List<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		Map<PageId, Page> dirtypages = new HashMap<>();

		// get a read lock on the root pointer page and use it to locate the root page 获取根指针页上的读锁，并使用它来定位根页
		BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
		BTreePageId rootId = rootPtr.getRootId();

		if(rootId == null) { // the root has just been created, so set the root pointer to point to it	根节点刚刚被创建，所以设置根指针指向它
			rootId = new BTreePageId(tableid, numPages(), BTreePageId.LEAF);
			rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
			rootPtr.setRootId(rootId);
		}

		// find and lock the left-most leaf page corresponding to the key field,
		// and split the leaf page if there are no more slots available
		// 找到并锁定与关键字段对应的最左边的叶子页面，
		// 如果没有更多可用槽位，则拆分叶子页面
		BTreeLeafPage leafPage = findLeafPage(tid, dirtypages, rootId, Permissions.READ_WRITE, t.getField(keyField));
		if(leafPage.getNumEmptySlots() == 0) {
			leafPage = splitLeafPage(tid, dirtypages, leafPage, t.getField(keyField));
		}

		// insert the tuple into the leaf page 将元组插入叶子页面
		leafPage.insertTuple(t);

		return new ArrayList<>(dirtypages.values());
	}

	/**
	 *
	 处理由于删除而导致B+树页面不足一半的情况。
	 如果它的一个兄弟元素有额外的元组/项，重新分配这些元组/项。
	 否则就合并其中一个兄弟姐妹。根据需要更新指针。
	 * 处理最小占用页面
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages 脏页列表，应该用所有新的脏页更新
	 * @param page - the page which is less than half full 未满一半的页面
	 * @see #handleMinOccupancyLeafPage(TransactionId, Map, BTreeLeafPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
	 * @see #handleMinOccupancyInternalPage(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void handleMinOccupancyPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePage page) throws DbException, IOException, TransactionAbortedException {
		BTreePageId parentId = page.getParentId();
		BTreeEntry leftEntry = null;
		BTreeEntry rightEntry = null;
		BTreeInternalPage parent = null;

		// find the left and right siblings through the parent so we make sure they have the same parent as the page.
		// Find the entries in the parent corresponding to the page and siblings
		// 通过父级找到左右兄弟姐妹，因此我们确保它们与页面具有相同的父级。 // 查找父页面和兄弟节点对应的条目
		if(parentId.pgcateg() != BTreePageId.ROOT_PTR) {
			parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId, Permissions.READ_WRITE);
			Iterator<BTreeEntry> ite = parent.iterator();
			while(ite.hasNext()) {
				BTreeEntry e = ite.next();
				if(e.getLeftChild().equals(page.getId())) {
					rightEntry = e;
					break;
				}
				else if(e.getRightChild().equals(page.getId())) {
					leftEntry = e;
				}
			}
		}

		if(page.getId().pgcateg() == BTreePageId.LEAF) {
			handleMinOccupancyLeafPage(tid, dirtypages, (BTreeLeafPage) page, parent, leftEntry, rightEntry);
		}
		else { // BTreePageId.INTERNAL
			handleMinOccupancyInternalPage(tid, dirtypages, (BTreeInternalPage) page, parent, leftEntry, rightEntry);
		}
	}

	/**
	 *
	 处理由于删除而导致叶页少于一半满的情况。
	 如果它的一个兄弟有额外的元组，重新分配这些元组。
	 否则就合并其中一个兄弟姐妹。根据需要更新指针。
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the leaf page which is less than half full
	 * @param parent - the parent of the leaf page
	 * @param leftEntry - the entry in the parent pointing to the given page and its left-sibling
	 * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
	 * @see #mergeLeafPages(TransactionId, Map, BTreeLeafPage, BTreeLeafPage, BTreeInternalPage, BTreeEntry)
	 * @see #stealFromLeafPage(BTreeLeafPage, BTreeLeafPage, BTreeInternalPage,  BTreeEntry, boolean)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void handleMinOccupancyLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeLeafPage page,
											BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
			throws DbException, IOException, TransactionAbortedException {
		BTreePageId leftSiblingId = null;
		BTreePageId rightSiblingId = null;
		if(leftEntry != null) {
			leftSiblingId = leftEntry.getLeftChild();
		}
		if(rightEntry != null) {
			rightSiblingId = rightEntry.getRightChild();
		}

		int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples()/2; // ceiling
		if(leftSiblingId != null) {
			BTreeLeafPage leftSibling = (BTreeLeafPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
			// if the left sibling is at minimum occupancy, merge with it. Otherwise
			// steal some tuples from it
			if(leftSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeLeafPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
			}
			else {
				stealFromLeafPage(page, leftSibling, parent, leftEntry, false);
			}
		}
		else if(rightSiblingId != null) {
			BTreeLeafPage rightSibling = (BTreeLeafPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
			// if the right sibling is at minimum occupancy, merge with it. Otherwise
			// steal some tuples from it
			if(rightSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeLeafPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
			}
			else {
				stealFromLeafPage(page, rightSibling, parent, rightEntry, true);
			}
		}
	}

	/**
	 * 从兄弟中窃取元组并将它们复制到给定的页面，这样两个页面都至少是半满的。
	 * 更新父项的条目，以便键匹配右侧页面中第一个元组的键字段
	 *
	 * @param page - the leaf page which is less than half full 未满一半的叶页
	 * @param sibling - the sibling which has tuples to spare 有多余元组的兄弟姐妹
	 * @param parent - the parent of the two leaf pages 两个叶子页面的父页面
	 * @param entry - the entry in the parent pointing to the two leaf pages 指向两个叶页的父项中的条目
	 * @param isRightSibling - whether the sibling is a right-sibling 兄弟姐妹是否是右兄弟姐妹
	 *
	 * @throws DbException
	 */
	public void stealFromLeafPage(BTreeLeafPage page, BTreeLeafPage sibling,
								  BTreeInternalPage parent, BTreeEntry entry, boolean isRightSibling) throws DbException {

		// 将一些元组从同级移动到页面，以便元组均匀分布,一定要更新相应的父条目
		int sibling_num = sibling.getNumTuples() + 1;
		int middleIndex = ((sibling.getMaxTuples() + 1) + (page.getMaxTuples() + 1)) / 2;
		Tuple splitTuple = null;

		Iterator<Tuple> fullPageIterator = sibling.iterator();
		BTreeLeafPage prev_page = null;
		BTreeLeafPage next_page = null;
		// start分配数组：  是把你挪到前面 还是把我的数据挪到你的后面
		if (page.getTuple(0).getField(0).compare(Op.LESS_THAN, sibling.getTuple(0).getField(0))) {
			// 表示page在前，把sibling头部的数据放到 page的尾部
//			prev_page = page;
//			next_page = sibling;
			int temp = middleIndex-sibling_num;
			int i = 0;
			while ( i < temp) {
				if (fullPageIterator.hasNext()) {
					Tuple targetTuple = fullPageIterator.next();
					page.insertTuple(targetTuple);
					// question: 如何保证顺序
					sibling.deleteTuple(targetTuple);
				}
				i++;
			}
		} else {
			// 表示page在后，把sibling尾巴的数据放到 page的前面
//			prev_page = sibling;
//			next_page = page;
			int temp = middleIndex-sibling_num;
			int i = middleIndex;	// 起始点copy为mid
			int count_num = middleIndex;
			Tuple targetTuple = null;
			while ( i < (middleIndex + temp) && fullPageIterator.hasNext() ) {
				targetTuple = fullPageIterator.next();
				i++;
			}

			while (fullPageIterator.hasNext()) {
				targetTuple = fullPageIterator.next();
				page.insertTuple(targetTuple);
				// question: 如何保证顺序
				sibling.deleteTuple(targetTuple);
			}
		}

		// 更新父条目
//		parent




	}

	/**
	 *
	 * 处理由于删除导致内部页面小于半满的情况。
	 * 如果它的同级之一有额外的条目，则重新分配这些条目。
	 * 否则与其中一个兄弟姐妹合并。根据需要更新指针。
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - 脏页列表，应该用所有新的脏页更新
	 * @param page - 不到半满的内部页面
	 * @param parent -	内部页面的父级
	 * @param leftEntry - 父项中指向给定页面及其左兄弟的条目
	 * @param rightEntry - 父项中指向给定页面及其右兄弟的条目
	 * 处理最小占用内部页面
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void handleMinOccupancyInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
												BTreeInternalPage page, BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
			throws DbException, IOException, TransactionAbortedException {
		BTreePageId leftSiblingId = null;
		BTreePageId rightSiblingId = null;
		if(leftEntry != null) {
			leftSiblingId = leftEntry.getLeftChild();
		}
		if(rightEntry != null) {
			rightSiblingId = rightEntry.getRightChild();
		}

		int maxEmptySlots = page.getMaxEntries() - page.getMaxEntries()/2; // ceiling
		if(leftSiblingId != null) {
			BTreeInternalPage leftSibling = (BTreeInternalPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
			// if the left sibling is at minimum occupancy, merge with it. Otherwise
			// steal some entries from it
			if(leftSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeInternalPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
			}
			else {
				stealFromLeftInternalPage(tid, dirtypages, page, leftSibling, parent, leftEntry);
			}
		}
		else if(rightSiblingId != null) {
			BTreeInternalPage rightSibling = (BTreeInternalPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
			// if the right sibling is at minimum occupancy, merge with it. Otherwise
			// steal some entries from it
			if(rightSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeInternalPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
			}
			else {
				stealFromRightInternalPage(tid, dirtypages, page, rightSibling, parent, rightEntry);
			}
		}
	}

	/**
	 * 从左兄弟中窃取条目并将它们复制到给定页面，以便两个页面都至少有一半是满的。
	 * key 可以被认为是通过父条目旋转，因此父条目中的原始键被“下拉”到右侧页面，
	 * 而左侧页面中的最后一个键被“向上推”到父条目。根据需要更新父指针。
	 *
	 * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
	 *
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	public void stealFromLeftInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
										  BTreeInternalPage page, BTreeInternalPage leftSibling, BTreeInternalPage parent,
										  BTreeEntry parentEntry) throws DbException, TransactionAbortedException {

		// 将一些条目从左兄弟移动到页面，以便条目均匀分布。
		// 一定要更新相应的父条目。确保更新已移动条目中所有子项的父指针。
	}

	/**
	 从右边的同级抓取条目并将它们复制到给定的页面，以便两个页面至少
	 半满的。可以将键看作是在父条目中旋转，因此家长被“拉下”到左边页，而右边页的最后一个键被“推上”的父母。
	 根据需要更新父指针。
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page which is less than half full
	 * @param rightSibling - the right sibling which has entries to spare
	 * @param parent - the parent of the two internal pages
	 * @param parentEntry - the entry in the parent pointing to the two internal pages
	 * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
	 *
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	public void stealFromRightInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
										   BTreeInternalPage page, BTreeInternalPage rightSibling, BTreeInternalPage parent,
										   BTreeEntry parentEntry) throws DbException, TransactionAbortedException {

		// 将一些条目从右侧兄弟移至页面，以便使条目均匀分布。一定要更新相应的父条目。确保更新已移动条目中所有子项的父指针。
	}

	/**
	 * 通过将所有元组从右侧页面移动到左侧页面来合并两个叶页面。
	 * 从父级中删除相应的键和右子指针，并递归处理父级低于最小占用率的情况。
	 * 根据需要更新同级指针，并使正确的页面可供重复使用。
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param leftPage - the left leaf page
	 * @param rightPage - the right leaf page
	 * @param parent - the parent of the two pages
	 * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
	 * @see #deleteParentEntry(TransactionId, Map, BTreePage, BTreeInternalPage, BTreeEntry)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public void mergeLeafPages(TransactionId tid, Map<PageId, Page> dirtypages,
							   BTreeLeafPage leftPage, BTreeLeafPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
			throws DbException, IOException, TransactionAbortedException {

		// 通过将所有元组从右侧页面移动到左侧页面来合并两个叶页面。
		// 从父级中删除相应的键和右子指针，并递归处理父级低于最小占用率的情况。
		// 根据需要更新同级指针，并使正确的页面可供重复使用。

		Iterator<Tuple> rightTuples = rightPage.iterator();
		while (rightTuples.hasNext()) {
			Tuple tuple = rightTuples.next();
			// 将从右叶子子页面读取出来的插入左叶子页面：存在问题，右子页面的极大值应该小于左叶子页面的极小值（实现见insertTuple）
			leftPage.insertTuple(tuple);
		}

		// 清除父页面的该条 entry
		deleteParentEntry(tid, dirtypages, leftPage, parent, parentEntry);

		// 设置右子界面为空 并加入dirtypage列表
		setEmptyPage(tid, dirtypages, rightPage.getId().getPageNumber());


		// clear parent page pointer 叶子的指针问题
	}

	/**
	 * 通过将所有条目从右页移动到左页并从父条目“下拉”相应键来合并两个内部页。
	 * 从父级中删除相应的键和右子指针，并递归处理父级低于最小占用率的情况。
	 * 根据需要更新父指针，并使正确的页面可供重复使用
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param leftPage - the left internal page
	 * @param rightPage - the right internal page
	 * @param parent - the parent of the two pages
	 * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
	 * @see #deleteParentEntry(TransactionId, Map, BTreePage, BTreeInternalPage, BTreeEntry)
	 * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public void mergeInternalPages(TransactionId tid, Map<PageId, Page> dirtypages,
								   BTreeInternalPage leftPage, BTreeInternalPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
			throws DbException, IOException, TransactionAbortedException {

		//将所有的条目从右页移动到左页，更新
		//被移动的项的子元素的父指针，
		//删除父页面中与正在合并的两个页面对应的条目

		Iterator<BTreeEntry> entryIterator = rightPage.iterator();
		while (entryIterator.hasNext()) {
			BTreeEntry right_page_entry = entryIterator.next();
			leftPage.insertEntry(right_page_entry);
		}

		// 将 parent page add to next level
		leftPage.insertEntry(parentEntry);

		// delete parent entry
		deleteParentEntry(tid, dirtypages, leftPage, parent, parentEntry);

		// 使正确的页面可重用
		setEmptyPage(tid, dirtypages,rightPage.getId().getPageNumber());


		updateParentPointers(tid, dirtypages, parent);
//		parent.getChildId()
//		leftPage.parent;

		// 处理最小占用内部页面


	}

	/**
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param leftPage - the child remaining after the key and right child are deleted
	 * @param parent - the parent containing the entry to be deleted
	 * @param parentEntry - the entry to be deleted
	 * @see #handleMinOccupancyPage(TransactionId, Map, BTreePage)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void deleteParentEntry(TransactionId tid, Map<PageId, Page> dirtypages,
								   BTreePage leftPage, BTreeInternalPage parent, BTreeEntry parentEntry)
			throws DbException, IOException, TransactionAbortedException {

		// delete the entry in the parent.  If
		// the parent is below minimum occupancy, get some tuples from its siblings
		// or merge with one of the siblings
		parent.deleteKeyAndRightChild(parentEntry);
		int maxEmptySlots = parent.getMaxEntries() - parent.getMaxEntries()/2; // ceiling
		if(parent.getNumEmptySlots() == parent.getMaxEntries()) {
			// This was the last entry in the parent.
			// In this case, the parent (root node) should be deleted, and the merged
			// page will become the new root
			BTreePageId rootPtrId = parent.getParentId();
			if(rootPtrId.pgcateg() != BTreePageId.ROOT_PTR) {
				throw new DbException("attempting to delete a non-root node");
			}
			BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, rootPtrId, Permissions.READ_WRITE);
			leftPage.setParentId(rootPtrId);
			rootPtr.setRootId(leftPage.getId());

			// release the parent page for reuse
			setEmptyPage(tid, dirtypages, parent.getId().getPageNumber());
		}
		else if(parent.getNumEmptySlots() > maxEmptySlots) {
			handleMinOccupancyPage(tid, dirtypages, parent);
		}
	}

	/**
	 * Delete a tuple from this BTreeFile.
	 * May cause pages to merge or redistribute entries/tuples if the pages
	 * become less than half full.
	 *
	 * @param tid - the transaction id
	 * @param t - the tuple to delete
	 * @return a list of all pages that were dirtied by this operation. Could include
	 * many pages since parent pointers will need to be updated when an internal node merges.
	 * @see #handleMinOccupancyPage(TransactionId, Map, BTreePage)
	 */
	@Override
	public List<Page> deleteTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		Map<PageId, Page> dirtypages = new HashMap<>();

		BTreePageId pageId = new BTreePageId(tableid, t.getRecordId().getPageId().getPageNumber(),
				BTreePageId.LEAF);
		BTreeLeafPage page = (BTreeLeafPage) getPage(tid, dirtypages, pageId, Permissions.READ_WRITE);
		page.deleteTuple(t);

		// if the page is below minimum occupancy, get some tuples from its siblings
		// or merge with one of the siblings
		int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples()/2; // ceiling
		if(page.getNumEmptySlots() > maxEmptySlots) {
			handleMinOccupancyPage(tid, dirtypages, page);
		}

		return new ArrayList<>(dirtypages.values());
	}

	/**
	 * 获得根指针页上的读锁。如果需要，创建根指针页和根页。
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @return the root pointer page
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	BTreeRootPtrPage getRootPtrPage(TransactionId tid, Map<PageId, Page> dirtypages) throws DbException, IOException, TransactionAbortedException {

		synchronized(this) {
			if(f.length() == 0) {
				// create the root pointer page and the root page 创建根指针页和根页
				BufferedOutputStream bw = new BufferedOutputStream(
						new FileOutputStream(f, true));
				byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
				byte[] emptyLeafData = BTreeLeafPage.createEmptyPageData();
				bw.write(emptyRootPtrData);
				bw.write(emptyLeafData);
				bw.close();
			}
		}

		// get a read lock on the root pointer page 获得根指针页上的读锁
		return (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_ONLY);
	}

	/**
	 *
	 * 获取这个BTreeFile中第一个空页的页码。
	 * 如果现有页均为空，则创建新页。
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @return the page number of the first empty page
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public int getEmptyPageNo(TransactionId tid, Map<PageId, Page> dirtypages)
			throws DbException, IOException, TransactionAbortedException {
		// get a read lock on the root pointer page and use it to locate the first header page 获取根指针页上的读锁，并使用它来定位第一个头页

		BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
		BTreePageId headerId = rootPtr.getHeaderId();
		int emptyPageNo = 0;

		if(headerId != null) {
			BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
			int headerPageCount = 0;
			// try to find a header page with an empty slot
			while(headerPage != null && headerPage.getEmptySlot() == -1) {
				headerId = headerPage.getNextPageId();
				if(headerId != null) {
					headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
					headerPageCount++;
				}
				else {
					headerPage = null;
				}
			}

			// if headerPage is not null, it must have an empty slot 如果headerPage不为空，它必须有一个空槽
			if(headerPage != null) {
				headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
				int emptySlot = headerPage.getEmptySlot();
				headerPage.markSlotUsed(emptySlot, true);
				emptyPageNo = headerPageCount * BTreeHeaderPage.getNumSlots() + emptySlot;
			}
		}

		// at this point if headerId is null, either there are no header pages 此时如果headerId为null
		// or there are no free slots 要么没有头页或者没有空闲插槽
		if(headerId == null) {
			synchronized(this) {
				// create the new page
				BufferedOutputStream bw = new BufferedOutputStream(
						new FileOutputStream(f, true));
				byte[] emptyData = BTreeInternalPage.createEmptyPageData();
				bw.write(emptyData);
				bw.close();
				emptyPageNo = numPages();
			}
		}

		return emptyPageNo;
	}

	/**
	 *
	 * 方法封装创建新页面的过程。如果可能的话，它可以重用旧页面，
	 * 如果没有可用的页面，则创建一个新页面。它会擦除磁盘和缓存中的页面
	 * 返回锁定并具有读写权限的干净副本
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pgcateg - the BTreePageId category of the new page.  Either LEAF, INTERNAL, or HEADER
	 * @return the new empty page
	 * @see #getEmptyPageNo(TransactionId, Map)
	 * @see #setEmptyPage(TransactionId, Map, int)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private Page getEmptyPage(TransactionId tid, Map<PageId, Page> dirtypages, int pgcateg)
			throws DbException, IOException, TransactionAbortedException {
		// create the new page

		int emptyPageNo = getEmptyPageNo(tid, dirtypages);
		BTreePageId newPageId = new BTreePageId(tableid, emptyPageNo, pgcateg);

		// write empty page to disk
		RandomAccessFile rf = new RandomAccessFile(f, "rw");
		rf.seek(BTreeRootPtrPage.getPageSize() + (long) (emptyPageNo - 1) * BufferPool.getPageSize());
		rf.write(BTreePage.createEmptyPageData());
		rf.close();

		// make sure the page is not in the buffer pool	or in the local cache
		Database.getBufferPool().discardPage(newPageId);
		dirtypages.remove(newPageId);

		return getPage(tid, dirtypages, newPageId, Permissions.READ_WRITE);
	}

	/**
	 * 在这个BTreeFile中将一个页面标记为空。找到相应的标题页(如果需要创建它)，并将标题页中相应的槽标记为空
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param emptyPageNo - the page number of the empty page
	 * @see #getEmptyPage(TransactionId, Map, int)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public void setEmptyPage(TransactionId tid, Map<PageId, Page> dirtypages, int emptyPageNo)
			throws DbException, IOException, TransactionAbortedException {

		// if this is the last page in the file (and not the only page), just
		// truncate the file
		// @TODO: Commented out because we should probably do this somewhere else in case the transaction aborts....
//		synchronized(this) {
//			if(emptyPageNo == numPages()) {
//				if(emptyPageNo <= 1) {
//					// if this is the only page in the file, just return.
//					// It just means we have an empty root page
//					return;
//				}
//				long newSize = f.length() - BufferPool.getPageSize();
//				FileOutputStream fos = new FileOutputStream(f, true);
//				FileChannel fc = fos.getChannel();
//				fc.truncate(newSize);
//				fc.close();
//				fos.close();
//				return;
//			}
//		}

		// otherwise, get a read lock on the root pointer page and use it to locate
		// the first header page
		//在根指针页上获取一个读锁，并使用它来定位第一个标题页
		BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
		BTreePageId headerId = rootPtr.getHeaderId();
		BTreePageId prevId = null;
		int headerPageCount = 0;

		// if there are no header pages, create the first header page and update
		// the header pointer in the BTreeRootPtrPage
		if(headerId == null) {
			rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);

			BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
			headerId = headerPage.getId();
			headerPage.init();
			rootPtr.setHeaderId(headerId);
		}

		// iterate through all the existing header pages to find the one containing the slot
		// corresponding to emptyPageNo
		while(headerId != null && (headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
			BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
			prevId = headerId;
			headerId = headerPage.getNextPageId();
			headerPageCount++;
		}

		// at this point headerId should either be null or set with
		// the headerPage containing the slot corresponding to emptyPageNo.
		// Add header pages until we have one with a slot corresponding to emptyPageNo
		while((headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
			BTreeHeaderPage prevPage = (BTreeHeaderPage) getPage(tid, dirtypages, prevId, Permissions.READ_WRITE);

			BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
			headerId = headerPage.getId();
			headerPage.init();
			headerPage.setPrevPageId(prevId);
			prevPage.setNextPageId(headerId);

			headerPageCount++;
			prevId = headerId;
		}

		// now headerId should be set with the headerPage containing the slot corresponding to
		// emptyPageNo
		BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
		int emptySlot = emptyPageNo - headerPageCount * BTreeHeaderPage.getNumSlots();
		headerPage.markSlotUsed(emptySlot, false);
	}

	/**
	 * 基于其IndexPredicate值从文件中获取指定的元组代表指定的事务。
	 * 这个方法将获得一个读锁文件中受影响的页面，并且可能会阻塞，直到获得锁为止。
	 *
	 * @param tid - the transaction id
	 * @param ipred - the index predicate value to filter on
	 * @return an iterator for the filtered tuples
	 */
	public DbFileIterator indexIterator(TransactionId tid, IndexPredicate ipred) {
		return new BTreeSearchIterator(this, tid, ipred);
	}

	/**
	 Get an iterator for all tuples in this B+ tree file in sorted order. This method
	 will acquire a read lock on the affected pages of the file, and may block until
	 the lock can be acquired.
	 *
	 * @param tid - the transaction id
	 * @return an iterator for all the tuples in this file
	 */
	@Override
	public DbFileIterator iterator(TransactionId tid) {
		return new BTreeFileIterator(this, tid);
	}

}

/**
 *
 * 为 BTreeFile 上的 tuple 实现 Java Iterator 的 Helper 类
 */
class BTreeFileIterator extends AbstractDbFileIterator {

	Iterator<Tuple> it = null;
	BTreeLeafPage curp = null;

	final TransactionId tid;
	final BTreeFile f;

	/**
	 * Constructor for this iterator
	 * @param f - the BTreeFile containing the tuples
	 * @param tid - the transaction id
	 */
	public BTreeFileIterator(BTreeFile f, TransactionId tid) {
		this.f = f;
		this.tid = tid;
	}

	/**
	 * 通过获取第一个叶页上的迭代器来打开此迭代器
	 */
	@Override
	public void open() throws DbException, TransactionAbortedException {
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
		BTreePageId root = rootPtr.getRootId();
		curp = f.findLeafPage(tid, root, null);
		it = curp.iterator();
	}

	/**
	 * 从当前页面读取下一个元组(如果它有更多元组)或从下一页跟随右同胞指针
	 *
	 * @return the next tuple, or null if none exists
	 */
	@Override
	protected Tuple readNext() throws TransactionAbortedException, DbException {
		if (it != null && !it.hasNext()) {
			it = null;
		}

		while (it == null && curp != null) {
			BTreePageId nextp = curp.getRightSiblingId();
			if(nextp == null) {
				curp = null;
			}
			else {
				curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
						nextp, Permissions.READ_ONLY);
				it = curp.iterator();
				if (!it.hasNext()) {
					it = null;
				}
			}
		}

		if (it == null) {
			return null;
		}
		return it.next();
	}

	/**
	 *  将迭代器倒回元组的开头
	 */
	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	/**
	 * close the iterator
	 */
	@Override
	public void close() {
		super.close();
		it = null;
		curp = null;
	}
}

/**
 *  为B+树文件上的搜索元组实现DbFileIterator的Helper类
 */
class BTreeSearchIterator extends AbstractDbFileIterator {

	Iterator<Tuple> it = null;
	BTreeLeafPage curp = null;

	final TransactionId tid;
	final BTreeFile f;
	final IndexPredicate ipred;

	/**
	 * 此迭代器的构造函数
	 * Constructor for this iterator
	 * @param f - the BTreeFile containing the tuples
	 * @param tid - the transaction id
	 * @param ipred - the predicate to filter on
	 */
	public BTreeSearchIterator(BTreeFile f, TransactionId tid, IndexPredicate ipred) {
		this.f = f;
		this.tid = tid;
		this.ipred = ipred;
	}

	/**
	 * 通过获取适用于给定谓词操作的第一个叶页上的迭代器来打开此迭代器
	 */
	@Override
	public void open() throws DbException, TransactionAbortedException {
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
		BTreePageId root = rootPtr.getRootId();
		if(ipred.getOp() == Op.EQUALS || ipred.getOp() == Op.GREATER_THAN
				|| ipred.getOp() == Op.GREATER_THAN_OR_EQ) {
			curp = f.findLeafPage(tid, root, ipred.getField());
		}
		else {
			curp = f.findLeafPage(tid, root, null);
		}
		it = curp.iterator();
	}

	/**
	 如果当前页有更多匹配谓词的元组，则从当前页读取下一个元组，或者通过跟随右兄弟指针从下一页读取下一个元组。
	 *
	 * @return the next tuple matching the predicate, or null if none exists
	 */
	@Override
	protected Tuple readNext() throws TransactionAbortedException, DbException,
			NoSuchElementException {
		while (it != null) {

			while (it.hasNext()) {
				Tuple t = it.next();
				if (t.getField(f.keyField()).compare(ipred.getOp(), ipred.getField())) {
					return t;
				}
				else if(ipred.getOp() == Op.LESS_THAN || ipred.getOp() == Op.LESS_THAN_OR_EQ) {
					// if the predicate was not satisfied and the operation is less than, we have
					// hit the end
					return null;
				}
				else if(ipred.getOp() == Op.EQUALS &&
						t.getField(f.keyField()).compare(Op.GREATER_THAN, ipred.getField())) {
					// if the tuple is now greater than the field passed in and the operation
					// is equals, we have reached the end
					return null;
				}
			}

			BTreePageId nextp = curp.getRightSiblingId();
			// if there are no more pages to the right, end the iteration
			if(nextp == null) {
				return null;
			}
			else {
				curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
						nextp, Permissions.READ_ONLY);
				it = curp.iterator();
			}
		}

		return null;
	}

	/**
	 *  将迭代器倒回元组的开头
	 */
	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	/**
	 * close the iterator
	 */
	@Override
	public void close() {
		super.close();
		it = null;
	}
}

