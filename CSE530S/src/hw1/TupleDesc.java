/**
 * name: Hao Zhang
 * id: 452003
 * wustlkey: h.zhang633
 * name: Hanming Li
 * id: 451802
 * wustlkey: lihanming
 */
package hw1;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

	private Type[] types;
	private String[] fields;
	
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	//your code here
    	int l = typeAr.length;
    	if(l != fieldAr.length){
    		System.out.println("The data is wrong");
    		return;
    	}
    	this.types = typeAr;
    	this.fields = fieldAr;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        //your code here
    	int l = this.fields.length;
    	return l;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        //your code here
    	String name = null;
    	try{
    		name = this.fields[i];
    		return name;
    	}catch(NoSuchElementException e){
    		throw e;
    	}
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        //your code here
    		for(int j = 0; j < this.fields.length; j++){
    			if(this.fields[j].equals(name)){
    				return j;
    			}
    		}
    		throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        //your code here
    	Type gType = null;
    	try{
    		  gType = this.types[i];
    		  return gType;
    	}catch(NoSuchElementException e){
    		throw e;
    	}
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	//your code here
    	int s = 0;
    	int l = this.types.length;
    	for(int i = 0; i < l; i++){
    		if(this.types[i] == Type.INT){
    			s += 4;
    		}
    		else if(this.types[i] == Type.STRING){
    			s += 129;
    		}
    	}
    	return s;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	//your code here
    	if(o.getClass() == this.getClass()){
    		TupleDesc c = (TupleDesc)o;
	    	if(this.getSize() != c.getSize()){
	    		return false;
	    	}
	    	else{
	    		if(c.types.length != this.types.length){
	    			return false;
	    		}
	    		for(int i = 0; i < c.types.length; i++){
	    			if(c.types[i] != this.types[i]){
	    				return false;
	    			}
	    		}
	    	}
    	}
    	return true;
    }
    

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        //your code here
    	String str = null;
    	for(int i = 0; i < this.types.length; i++){
    		String c = null;
    		if(this.types[i] == Type.INT){
    			c = "int";
    		}
    		else if(this.types[i] == Type.STRING){
    			c = "String";
    		}
    		str += c + '(' + this.fields[i] + ')';
    	}
    	return str;
    }
}
