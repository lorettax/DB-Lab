package tinydb.common;

import tinydb.storage.*;

import java.io.*;
import java.util.List;
import java.util.UUID;

/** 用于测试和实现随机特征的辅助方法 */
public class Utility {
    /**
     * @return a Type array of length len populated with Type.INT_TYPE
     */
    public static Type[] getTypes(int len) {
        Type[] types = new Type[len];
        for (int i = 0; i < len; ++i) {
            types[i] = Type.INT_TYPE;
        }
        return types;
    }


    public static String[] getStrings(int len, String val) {
        String[] strings = new String[len];
        for (int i = 0; i < len; ++i) {
            strings[i] = val + i;
        }
        return strings;
    }

    public static TupleDesc getTupleDesc(int n, String name) {
        return new TupleDesc(getTypes(n), getStrings(n, name));
    }

    /**
     * @return a TupleDesc with n fields of type Type.INT_TYPE
     */
    public static TupleDesc getTupleDesc(int n) {
        return new TupleDesc(getTypes(n));
    }

    /**
     * @return a Tuple with a single IntField with value n and with RecordId(HeapPageId(1,2), 3)
     */
    public static Tuple getHeapTuple(int n) {
        Tuple tup = new Tuple(getTupleDesc(1));
        tup.setRecordId(new RecordId(new HeapPageId(1, 2), 3));
        tup.setField(0, new IntField(n));
        return tup;
    }

    /**
     * @return a Tuple with an IntField for every element of tupdata
     *   and RecordId(HeapPageId(1, 2), 3)
     */
    public static Tuple getHeapTuple(int[] tupdata) {
        Tuple tup = new Tuple(getTupleDesc(tupdata.length));
        tup.setRecordId(new RecordId(new HeapPageId(1, 2), 3));
        for (int i = 0; i < tupdata.length; ++i) {
            tup.setField(i, new IntField(tupdata[i]));
        }
        return tup;
    }

    /**
     * @return a Tuple with a 'width' IntFields each with value n and
     *   with RecordId(HeapPageId(1, 2), 3)
     */
    public static Tuple getHeapTuple(int n, int width) {
        Tuple tup = new Tuple(getTupleDesc(width));
        tup.setRecordId(new RecordId(new HeapPageId(1, 2), 3));
        for (int i = 0; i < width; ++i)
            tup.setField(i, new IntField(n));
        return tup;
    }

    /**
     * @return a Tuple with a 'width' IntFields with the value tupledata[i]
     *         in each field.
     *         do not set it's RecordId, hence do not distinguish which
     *         sort of file it belongs to.
     */
    public static Tuple getTuple(int[] tupledata, int width) {
        if(tupledata.length != width) {
            System.out.println("get Hash Tuple has the wrong length~");
            System.exit(1);
        }
        Tuple tup = new Tuple(getTupleDesc(width));
        for (int i = 0; i < width; ++i)
            tup.setField(i, new IntField(tupledata[i]));
        return tup;
    }

    public static HeapFile createEmptyHeapFile(String path, int cols)
        throws IOException {
        File f = new File(path);
        // touch the file
        FileOutputStream fos = new FileOutputStream(f);
        fos.write(new byte[0]);
        fos.close();

        HeapFile hf = openHeapFile(cols, f);
        HeapPageId pid = new HeapPageId(hf.getId(), 0);

        HeapPage page = null;
        try {
            page = new HeapPage(pid, HeapPage.createEmptyPageData());
        } catch (IOException e) {
            // this should never happen for an empty page; bail;
            throw new RuntimeException("failed to create empty page in HeapFile");
        }

        hf.writePage(page);
        return hf;
    }

    /** Open a HeapFile and adds it to the catalog.
     *
     * @param cols number of columns in the table.
     * @param f location of the file storing the table.
     * @return the opened table.
     */
    public static HeapFile openHeapFile(int cols, File f) {
        // create the HeapFile and add it to the catalog
    	TupleDesc td = getTupleDesc(cols);
        HeapFile hf = new HeapFile(f, td);
        Database.getCatalog().addTable(hf, UUID.randomUUID().toString());
        return hf;
    }

    public static HeapFile openHeapFile(int cols, String colPrefix, File f, TupleDesc td) {
        // create the HeapFile and add it to the catalog
        HeapFile hf = new HeapFile(f, td);
        Database.getCatalog().addTable(hf, UUID.randomUUID().toString());
        return hf;
    }
    
    public static HeapFile openHeapFile(int cols, String colPrefix, File f) {
        // create the HeapFile and add it to the catalog
    	TupleDesc td = getTupleDesc(cols, colPrefix);
    	return openHeapFile(cols, colPrefix, f, td);
    }

    public static String listToString(List<Integer> list) {
        StringBuilder out = new StringBuilder();
        for (Integer i : list) {
            if (out.length() > 0) out.append("\t");
            out.append(i);
        }
        return out.toString();
    }
}

