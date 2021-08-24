package tinydb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * 元组维护有关元组内容的信息
 * 元组具有由 TupleDesc 对象指定的指定模式，并包含带有每个字段数据的 Field 对象。
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tupleDesc;
    private RecordId recordId;
    private final Field[] fields;


    /**
     * 使用指定的模式（类型）创建一个新元组
     *
     * @param td
     * 这个tuple的模式。它必须是具有至少一个字段的有效 TupleDesc 实例。
     */
    public Tuple(TupleDesc td) {
        tupleDesc = td;
        fields = new Field[td.numFields()];
    }

    /**
     * @return TupleDesc 表示此tuple的模式。
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return 表示此元组在磁盘上的位置的 RecordId。可能为null
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * 设置此元组的 RecordId 信息
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordId = rid;
    }

    /**
     * 更改此元组的第 i 个字段的值
     * @param i 要更改的字段的索引必须是一个有效的索引
     * @param f field 的新值
     */
    public void setField(int i, Field f) {
        fields[i] = f;
    }

    /**
     * @return 第 i 个字段的值, 如果尚未设置, 则为 null
     * @param i field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields[i];
    }

    /**
     * 以字符串形式返回此 tuple 的内容.
     * the format needs to be as follows:
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    @Override
    public String toString() {
        StringBuilder sb =  new StringBuilder();
        for(int i=0;i<tupleDesc.numFields()-1;++i){
            sb.append(fields[i].toString()+" ");
        }
        sb.append(fields[tupleDesc.numFields()-1].toString()+"\n");
        return sb.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return (Iterator<Field>) Arrays.asList(fields).iterator();

    }

    /**
     * reset 此 tuple 的 TupleDesc（仅影响 TupleDesc）
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        tupleDesc = td;
    }
}
