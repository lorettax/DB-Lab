package tinydb.optimizer;

import tinydb.common.Database;
import tinydb.common.Type;
import tinydb.execution.Predicate;
import tinydb.execution.SeqScan;
import tinydb.storage.*;
import tinydb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats表示查询中关于基表的统计信息(例如，柱状图)
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     *  100, though our tests assume that have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int ioCostPerPage;
    private DbFile dbFile;
    private int tableid;
    private int numFields;
    private int numTuples;
    private int numPages;
    private HashMap<Integer,IntHistogram> intHistogramHashMap;
    private HashMap<Integer,StringHistogram> stringHistogramHashMap;

    /**
     * 创建一个新的 TableStats 对象，用于跟踪表中每一列的统计信息
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage
     *  The cost per page of IO. This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {

        numTuples = 0;
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        intHistogramHashMap = new HashMap<Integer, IntHistogram>();
        stringHistogramHashMap = new HashMap<Integer, StringHistogram>();

        dbFile = Database.getCatalog().getDatabaseFile(tableid);
        numPages = ((HeapFile)dbFile).numPages();
        TupleDesc td = dbFile.getTupleDesc();

        numFields = td.numFields();
        Type types[] = getTypes(td);

        int[] mins = new int[numFields];
        int[] maxs = new int[numFields];

        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid,tableid,"");
        try{
            scan.open();
            for(int i=0;i<numFields;++i){
                if(types[i] == Type.STRING_TYPE) {
                    continue;
                }

                int min = Integer.MAX_VALUE;
                int max = Integer.MIN_VALUE;

                while(scan.hasNext()){
                    if(i == 0) {
                        numTuples++;
                    }
                    Tuple tuple = scan.next();
                    IntField field = (IntField)tuple.getField(i);
                    int val = field.getValue();
                    if(val > max) {
                        max = val;
                    }
                    if(val < min) {
                        min = val;
                    }
                }
                scan.rewind();
                mins[i] = min;
                maxs[i] = max;
            }
            scan.close();
        }catch (Exception e){
            e.printStackTrace();
        }

        for(int i=0;i < numFields;++i){
            Type type = types[i];
            if(type == Type.INT_TYPE){
                IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS,mins[i],maxs[i]);
                intHistogramHashMap.put(i,intHistogram);
            }else{
                StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
                stringHistogramHashMap.put(i,stringHistogram);
            }
        }

        addValueToHist();
    }


    private Type[] getTypes(TupleDesc td){
        int numFields = td.numFields();
        Type[] types = new Type[numFields];

        for(int i=0;i<numFields;++i){
            Type t = td.getFieldType(i);
            types[i] = t;
        }
        return types;
    }

    private void addValueToHist(){
        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid,tableid,"");
        try{
            scan.open();
            while(scan.hasNext()){
                Tuple tuple = scan.next();

                for(int i=0;i<numFields;++i){
                    Field field = tuple.getField(i);

                    if(field.getType() == Type.INT_TYPE){
                        int val = ((IntField)field).getValue();
                        intHistogramHashMap.get(i).addValue(val);
                    }else{
                        String val = ((StringField)field).getValue();
                        stringHistogramHashMap.get(i).addValue(val);
                    }
                }
            }
            scan.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public double estimateScanCost() {
        HeapFile heapFile = (HeapFile)dbFile;
        return heapFile.numPages() * ioCostPerPage;
    }


    public int estimateTableCardinality(double selectivityFactor) {
        double cardinality = numTuples * selectivityFactor;
        return (int) cardinality;
    }

    public double avgSelectivity(int field, Predicate.Op op) {
        return 1.0;
    }


    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {

        double selectivity;
        if(constant.getType() == Type.INT_TYPE){
            IntField intField = (IntField) constant;
            selectivity = intHistogramHashMap.get(field).estimateSelectivity(op,intField.getValue());
        }else{
            StringField stringField = (StringField) constant;
            selectivity = stringHistogramHashMap.get(field).estimateSelectivity(op,stringField.getValue());
        }
        return selectivity;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return numTuples;
    }

}
