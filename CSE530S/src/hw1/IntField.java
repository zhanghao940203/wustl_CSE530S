/**
 * name: Hao Zhang
 * id: 452003
 * wustlkey: h.zhang633
 * name: Hanming Li
 * id: 451802
 * wustlkey: lihanming
 */
package hw1;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Instance of Field that stores a single integer.
 */
public class IntField implements Field {
    private int value;

    public int getValue() {
        return value;
    }

    /**
     * Constructor.
     *
     * @param i The value of this field.
     */
    public IntField(int i) {
        value = i;
    }
    
    public IntField(byte[] b) {
    	value = java.nio.ByteBuffer.wrap(b).getInt();
    }

    public String toString() {
        return Integer.toString(value);
    }

    public int hashCode() {
        return value;
    }

    public boolean equals(Object field) {
        return ((IntField) field).value == value;
    }

    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeInt(value);
    }
    
    public byte[] toByteArray() {
    	return ByteBuffer.allocate(4).putInt(value).array();
    }

    /**
     * Compare the specified field to the value of this Field.
     * Return semantics are as specified by Field.compare
     *
     * @throws IllegalCastException if val is not an IntField
     * @see Field#compare
     */
    public boolean compare(RelationalOperator op, Field val) {

        IntField iVal = (IntField) val;

        switch (op) {
        case EQ:
            return value == iVal.value;
        case NOTEQ:
            return value != iVal.value;

        case GT:
            return value > iVal.value;

        case GTE:
            return value >= iVal.value;

        case LT:
            return value < iVal.value;

        case LTE:
            return value <= iVal.value;
        }

        return false;
    }

    /**
     * Return the Type of this field.
     * @return Type.INT_TYPE
     */
	public Type getType() {
		return Type.INT;
	}
}
