package tinydb.storage;

import tinydb.transaction.TransactionId;

/**
 * Page is the interface used to represent pages that are resident in the
  BufferPool.  DbFiles will read and write pages from disk.
 */
public interface Page {


    PageId getId();

    /**
     * 获取上次弄脏此页面的事务的 ID，如果页面是干净的，则为 null
     */
    TransactionId isDirty();

  void markDirty(boolean dirty, TransactionId tid);

  /**
   *生成表示此页面内容的字节数组。用于将此页面序列化到磁盘
   * @return  一个字节数组对应于该页的字节
   */

  byte[] getPageData();


    Page getBeforeImage();

    void setBeforeImage();
}
