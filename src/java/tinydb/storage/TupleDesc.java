package tinydb.storage;

import tinydb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {


    private final TDItem[] tdItems;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /* The type of the field */
        public final Type fieldType;
        
        /* The name of the field */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return (Iterator<TDItem>) Arrays.asList(tdItems).iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * @param typeAr array specifying the number of and types of fields in this
     *   TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        tdItems = new TDItem[typeAr.length];
        for(int i=0;i<typeAr.length;++i){
            tdItems[i] = new TDItem(typeAr[i],fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr 参考上
     */
    public TupleDesc(Type[] typeAr) {
        tdItems = new TDItem[typeAr.length];
        for(int i=0;i<typeAr.length;++i){
            tdItems[i] = new TDItem(typeAr[i],"");
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItems.length;

    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if(i<0 || i>= tdItems.length){
            throw new NoSuchElementException("pos " + i + " is not a valid index");
        }
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if(i<0 || i>=tdItems.length){
            throw new NoSuchElementException("pos " + i + " is not a valid index");
        }
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {

        for(int i=0;i<tdItems.length;++i){
            if(tdItems[i].fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException("not find fieldName " + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for(int i=0;i<tdItems.length;++i){
            size += tdItems[i].fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] typeAr = new Type[td1.numFields() + td2.numFields()];
        String[] fieldAr = new String[td1.numFields() + td2.numFields()];
        for(int i=0;i<td1.numFields();++i){
            typeAr[i] = td1.tdItems[i].fieldType;
            fieldAr[i] = td1.tdItems[i].fieldName;
        }
        for(int i=0;i<td2.numFields();++i){
            typeAr[i+td1.numFields()] = td2.tdItems[i].fieldType;
            fieldAr[i+td1.numFields()] = td2.tdItems[i].fieldName;
        }
        return new TupleDesc(typeAr,fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    @Override
    public boolean equals(Object o) {
        if(this.getClass().isInstance(o)) {
            TupleDesc two = (TupleDesc) o;
            if (numFields() == two.numFields()) {
                for (int i = 0; i < numFields(); ++i) {
                    if (!tdItems[i].fieldType.equals(two.tdItems[i].fieldType)) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<tdItems.length-1;++i){
            sb.append(tdItems[i].fieldName + "(" + tdItems[i].fieldType + "), ");
        }
        sb.append(tdItems[tdItems.length-1].fieldName + "(" + tdItems[tdItems.length-1].fieldType + ")");
        return sb.toString();
    }
}
