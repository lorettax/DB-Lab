package tinydb.common;

import tinydb.storage.BufferPool;
import tinydb.storage.LogFile;

import java.io.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 数据库是一个初始化数据库系统使用的几个静态变量的类（特别是目录、缓冲池和日志文件）
 * 提供一组可用于从任何地方访问这些变量的方法
 * 
 * @Threadsafe
 */
public class Database {
    private static final AtomicReference<Database> _instance = new AtomicReference<>(new Database());
    private final Catalog _catalog;
    private final BufferPool _bufferpool;

    private final static String LOGFILENAME = "log";
    private final LogFile _logfile;

    private Database() {
        _catalog = new Catalog();
        _bufferpool = new BufferPool(BufferPool.DEFAULT_PAGES);
        LogFile tmp = null;
        try {
            tmp = new LogFile(new File(LOGFILENAME));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        _logfile = tmp;
        // startControllerThread();
    }

    /** Return the log file of the static Database instance */
    public static LogFile getLogFile() {
        return _instance.get()._logfile;
    }

    /** Return the buffer pool of the static Database instance */
    public static BufferPool getBufferPool() {
        return _instance.get()._bufferpool;
    }

    /** Return the catalog of the static Database instance */
    public static Catalog getCatalog() {
        return _instance.get()._catalog;
    }

    /**
     * Method used for testing -- create a new instance of the buffer pool and
     * return it
     */
    public static BufferPool resetBufferPool(int pages) {
        java.lang.reflect.Field bufferPoolF=null;
        try {
            bufferPoolF = Database.class.getDeclaredField("_bufferpool");
            bufferPoolF.setAccessible(true);
            bufferPoolF.set(_instance.get(), new BufferPool(pages));
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }
//        _instance._bufferpool = new BufferPool(pages);
        return _instance.get()._bufferpool;
    }

    // reset the database, used for unit tests only.
    public static void reset() {
        _instance.set(new Database());
    }

}
