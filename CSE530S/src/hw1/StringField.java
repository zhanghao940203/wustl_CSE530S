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
 * Instance of Field that stores a single String of a fixed length.
 */
public class StringField implements Field {
    private String value;
    private static final int maxSize = 128;

    public String getValue() {
        return value;
    }

    /**
     * Constructor.
     *
     * @param s The value of this field.
     * @param maxSize The maximum size of this string

     */
    public StringField(String s) {

    if (s.length() > maxSize)
        value = s.substring(0,maxSize);
    else
        value = s;
    }
    
    public StringField(byte[] b) {
		int len = b[0];
		char[] s = new char[len];
		for(int j = 1; j < len; j++) {
			s[j] = (char)b[j];
		}
		String s2 = new String(s);
		value = s2;
    }

    public String toString() {
        return value;
    }

    public int hashCode() {
        return value.hashCode();
    }

    public boolean equals(Object field) {
        return ((StringField) field).value.equals(value);
    }

    /** Write this string to dos.  Always writes maxSize + 4 bytes to the
    passed in dos.  First four bytes are string length, next bytes are
    string, with remainder padded with 0 to maxSize.
    @param dos Where the string is written
    */
    public void serialize(DataOutputStream dos) throws IOException {
    String s = value;
    int overflow = maxSize - s.length();
    if (overflow < 0) {
        String news = s.substring(0,maxSize);
        s  = news;
    }
    dos.writeInt(s.length());
    dos.writeBytes(s);
    while (overflow-- > 0)
        dos.write((byte)0);
    }

    /**
     * Compare the specified field to the value of this Field.
     * Return semantics are as specified by Field.compare
     *
     * @throws IllegalCastException if val is not a StringField
     * @see Field#compare
     */
    public boolean compare(RelationalOperator op, Field val) {

        StringField iVal = (StringField) val;
        int cmpVal = value.compareTo(iVal.value);

        switch (op) {
        case EQ:
            return cmpVal == 0;

        case NOTEQ:
            return cmpVal != 0;

        case GT:
            return cmpVal > 0;

        case GTE:
            return cmpVal >= 0;

        case LT:
            return cmpVal < 0;

        case LTE:
            return cmpVal <= 0;
        }

        return false;
    }
    /**
     * @return the Type for this Field
     */
	public Type getType() {
		
		return Type.STRING;
	}
	
	public byte[] toByteArray() {
		byte[] result = new byte[129];
		result[0] = (byte)value.length();
		for(int i = 0; i < value.length(); i++) {
			result[i + 1] = (byte)value.charAt(i);
		}
		return result;
	}
}
