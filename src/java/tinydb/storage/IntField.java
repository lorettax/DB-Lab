package tinydb.storage;

import tinydb.execution.Predicate;
import tinydb.common.Type;

import java.io.*;

/**
 * Instance of Field that stores a single integer.
 */
public class IntField implements Field {

    private static final long serialVersionUID = 1L;

    private final int value;

    public int getValue() {
        return value;
    }

    /**
     * @param i The value of this field.
     */
    public IntField(int i) {
        value = i;
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public boolean equals(Object field) {
        if (!(field instanceof IntField)) {
            return false;
        }
        return ((IntField) field).value == value;
    }

    @Override
    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeInt(value);
    }

    /**
     * Compare the specified field to the value of this Field.
     * Return semantics are as specified by Field.compare
     */
    @Override
    public boolean compare(Predicate.Op op, Field val) {

        IntField iVal = (IntField) val;

        switch (op) {
            case EQUALS:
            case LIKE:
                return value == iVal.value;
            case NOT_EQUALS:
                return value != iVal.value;
            case GREATER_THAN:
                return value > iVal.value;
            case GREATER_THAN_OR_EQ:
                return value >= iVal.value;
            case LESS_THAN:
                return value < iVal.value;
            case LESS_THAN_OR_EQ:
                return value <= iVal.value;
        }

        return false;
    }

    /**
     * Return the Type of this field.
     * @return Type.INT_TYPE
     */
    @Override
    public Type getType() {
        return Type.INT_TYPE;
    }
}
