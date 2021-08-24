package tinydb.storage;

import tinydb.execution.Predicate;
import tinydb.common.Type;

import java.io.*;

/**
 * Interface for values of fields in tuples in DB.
 */
public interface Field extends Serializable{
    /**
     * 将表示此字段的字节写入指定的 DataOutputStream.
     * @see DataOutputStream
     * @param dos 要写入的 DataOutputStream
     */
    void serialize(DataOutputStream dos) throws IOException;

    /**
     * 将此字段对象的值与传入的值进行compare比较
     * @param op The operator
     * @param value 与此字段进行比较的值
     * @return 比较结果是否为真
     */
    boolean compare(Predicate.Op op, Field value);

    /**
     * @return 此字段的类型
     */
    Type getType();
    
    /**
     * Hash code.
     * 表示相同值的不同 Field 对象可能应该返回相同的 hashCode
     */
    @Override
    int hashCode();

    @Override
    boolean equals(Object field);

    @Override
    String toString();
}
