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

/**
 * Interface for values of fields in tuples in SimpleDB.
 */
public interface Field {
    /**
     * Write the bytes representing this field to the specified
     * DataOutputStream.
     * @see DataOutputStream
     * @param dos The DataOutputStream to write to.
     */
    void serialize(DataOutputStream dos) throws IOException;

    /**
     * Compare the value of this field object to the passed in value.
     * @param op The operator
     * @param value The value to compare this Field to
     * @return Whether or not the comparison yields true.
     */
    public boolean compare(RelationalOperator op, Field value);

    /**
     * Returns the type of this field (see {@link Type#INT_TYPE} or {@link Type#STRING_TYPE}
     * @return type of this field
     */
    public Type getType();
    
    /**
     * Hash code.
     * Different Field objects representing the same value should probably
     * return the same hashCode.
     */
    public int hashCode();
    public boolean equals(Object field);
    
    public byte[] toByteArray();

    public String toString();
}
