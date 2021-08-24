package tinydb.execution;

import tinydb.storage.Field;
import tinydb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Constants used for return codes in Field.compare */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         * 
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        @Override
        public String toString() {
            if (this == EQUALS) {
                return "=";
            }
            if (this == GREATER_THAN) {
                return ">";
            }
            if (this == LESS_THAN) {
                return "<";
            }
            if (this == LESS_THAN_OR_EQ) {
                return "<=";
            }
            if (this == GREATER_THAN_OR_EQ) {
                return ">=";
            }
            if (this == LIKE) {
                return "LIKE";
            }
            if (this == NOT_EQUALS) {
                return "<>";
            }
            throw new IllegalStateException("impossible to reach here");
        }

    }


    private final int field;
    private final Op op;
    private final Field operand;

    /**
     * @param field field number of passed in tuples to compare against.
     * @param op operation to use for comparison
     * @param operand field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        this.field = field;
        this.op = op;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField()
    {
        return field;
    }

    /**
     * @return the operator
     */
    public Op getOp()
    {
        return op;
    }
    
    /**
     * @return the operand
     */
    public Field getOperand()
    {
        return operand;
    }
    
    /**
     * 使用构造函数中特定的运算符将构造函数中指定的 t 的字段编号与构造函数中指定的操作数字段进行比较。
     * 可以通过 Field 的 compare 方法进行比较。
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        return t.getField(field).compare(op,operand);
    }

    @Override
    public String toString() {
        String s = String.format("f = %d op = %s operand = %s", field,op.toString(),operand.toString());
        return s;
    }
}
