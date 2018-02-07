/**
 * name: Hao Zhang
 * id: 452003
 * wustlkey: h.zhang633
 * name: Hanming Li
 * id: 451802
 * wustlkey: lihanming
 */
package hw1;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {

	TupleDesc T;
	int Pid;
	int id;
	HashMap<String, byte[]> m = new HashMap<String, byte[]>();
	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	public Tuple(TupleDesc t) {
		//your code here
		this.T = t;
		Pid = 0;
		id = 0;
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.T;
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {
		//your code here
		return this.Pid;
	}

	public void setPid(int pid) {
		//your code here
		this.Pid = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		//your code here
		return this.id;
	}

	public void setId(int id) {
		//your code here
		this.id = id;
	}
	
	public void setDesc(TupleDesc td) {
		//your code here;
		this.T = td;
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, byte[] v) {
		//your code here
		String field = this.T.getFieldName(i);
		m.put(field, v);
	}
	
	public byte[] getField(int i) {
		//your code here
		String field = this.T.getFieldName(i);
		return m.get(field);
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		//your code here
		String str = "";
		for(int i = 0; i < this.T.numFields(); i++){
			byte[] v = m.get(this.T.getFieldName(i));
			if(v.length == 4){
				int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));  
				str += this.T.getFieldName(i) + '(' + n + ')' + ',';
			}
			else{
				String s = new String(v);
				str += this.T.getFieldName(i) + '(' + s + ')' + ',';
			}
		}
		return str;
	}
}
	