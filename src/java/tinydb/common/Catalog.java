package tinydb.common;

import tinydb.storage.DbFile;
import tinydb.storage.HeapFile;
import tinydb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  * Catalog 会跟踪数据库中所有可用的表及其关联的模式。
 * 
 * @Threadsafe
 */
public class Catalog {


    private final ConcurrentHashMap<Integer,Table> hashTable;

    private static class Table{
        private static final long serialVersionUID = 1L;

        public final DbFile dbFile;
        public final String tableName;
        public final String pk;

        public Table(DbFile file,String name,String pkeyField){
            dbFile = file;
            tableName = name;
            pk = pkeyField;
        }

        @Override
        public String toString(){
            return tableName + "(" + dbFile.getId() + ":" + pk +")";
        }
    }


    public Catalog() {
        hashTable = new ConcurrentHashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        Table t = new Table(file,name,pkeyField);
        hashTable.put(file.getId(),t);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }


    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     */
    public int getTableId(String name) throws NoSuchElementException {
        Integer res = hashTable.searchValues(1,value->{
            if(value.tableName.equals(name)){
                return value.dbFile.getId();
            }
            return null;
        });
        if(res != null){
            return res.intValue();
        }else{
            throw new NoSuchElementException("not found id for table " + name);
        }

    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId() function passed to addTable
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        Table t = hashTable.getOrDefault(tableid,null);
        if(t != null){
            return t.dbFile.getTupleDesc();
        }else{
            throw new NoSuchElementException("not found tuple desc for table " + tableid);
        }
    }

    /**
     * Returns the DbFile that can be used to read the contents of the specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId() function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        Table t = hashTable.getOrDefault(tableid,null);
        if(t != null){
            return t.dbFile;
        }else{
            throw new NoSuchElementException("not found db file for table " + tableid);
        }
    }

    public String getPrimaryKey(int tableid) {
        Table t = hashTable.getOrDefault(tableid,null);
        if(t != null){
            return t.pk;
        }else{
            throw new NoSuchElementException("not found primary key for table " + tableid);
        }
    }

    public Iterator<Integer> tableIdIterator() {
        return hashTable.keySet().iterator();
    }

    public String getTableName(int id) {
        Table t = hashTable.getOrDefault(id,null);
        if(t != null){
            return t.tableName;
        }else{
            throw new NoSuchElementException("not found name for table " + id);
        }
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        hashTable.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int")) {
                        types.add(Type.INT_TYPE);
                    } else if (els2[1].trim().equalsIgnoreCase("string")) {
                        types.add(Type.STRING_TYPE);
                    } else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk")) {
                            primaryKey = els2[0].trim();
                        } else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

