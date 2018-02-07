/**
 * name: Hao Zhang
 * id: 452003
 * wustlkey: h.zhang633
 * name: Hanming Li
 * id: 451802
 * wustlkey: lihanming
 */
package hw1;

import java.util.ArrayList;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		Type tp1 = td.getType(field);
		Type tp2 = operand.getType();
		String[] newname = new String[1];
		Type[] types = new Type[1];
		if(tp1 != tp2){
			return null;
		}
		ArrayList<Tuple> seltuples = new ArrayList<Tuple>();
		for(int i = 0; i < tuples.size(); i++){
			if(tp1 == Type.INT){
				//System.out.println("2");
				IntField f = new IntField(tuples.get(i).getField(field));
				if(f.compare(op, operand)){
					seltuples.add(tuples.get(i));
					//System.out.println("1");
				}
			}
			else if(tp1 == Type.STRING){
				//System.out.println("2");
				StringField f = new StringField(tuples.get(i).getField(field));
				if(f.compare(op, operand)){
					seltuples.add(tuples.get(i));
					//System.out.println("1");
				}
			}
		}
		return new Relation(seltuples, td);
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		//your code here
		 
		String[] newname = new String[td.numFields()];
		Type[] types = new Type[td.numFields()];
		for(int i = 0; i < td.numFields(); i++){
			types[i] = td.getType(i);
			newname[i] = td.getFieldName(i);
			for(int j = 0; j < fields.size(); j++){
				if(fields.get(j) == i){
					newname[i] = names.get(j);
					break;
				}
			}
		}
		TupleDesc td2 = new TupleDesc(types, newname);
		return new Relation(this.tuples, td2);
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
		String[] newname = new String[fields.size()];
		Type[] types = new Type[fields.size()];
		for(int i = 0; i < fields.size(); i++){
			newname[i] = td.getFieldName(fields.get(i));
			types[i] = td.getType(fields.get(i));
		}
		TupleDesc td2 = new TupleDesc(types, newname);
		ArrayList<Tuple> newtuples = new ArrayList<Tuple>();
		for(int i = 0; i < tuples.size(); i++){
			Tuple t = new Tuple(td2);
			for(int j = 0; j < td2.numFields(); j++){
				t.setField(j, tuples.get(i).getField(fields.get(j)));
			}
			newtuples.add(t);
		}
		
		return new Relation(newtuples, td2);
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
		String[] newname = new String[td.numFields() + other.td.numFields()];
		Type[] types = new Type[td.numFields() + other.td.numFields()];
		for(int i = 0; i < td.numFields() + other.td.numFields(); i++){
			if(i < td.numFields()){
				newname[i] = td.getFieldName(i);
				types[i] = td.getType(i);
			}
			else{
				newname[i] = other.td.getFieldName(i-td.numFields());
				types[i] = other.td.getType(i - td.numFields());
			}
		}
		TupleDesc td2 = new TupleDesc(types, newname);
		//System.out.println("New td size is: " + td2.getSize());
		int l1 = this.tuples.size();
		int l2 = other.tuples.size();
		//System.out.println(l1 + " and " + l2);
//		int l = 0;
//		int m = 0;
//		if(l1 > l2){
//			l = l2;
//			m = l1;
//		}else{
//			l = l1;
//			m = l2;
//		}
		ArrayList<Tuple> newtuples = new ArrayList<Tuple>();
		for(int i = 0; i < l1; i++){
			for(int j = 0; j < l2; j++){
				Field f1, f2;
				if(this.td.getType(field1) == Type.INT){
					f1 = new IntField(tuples.get(i).getField(field1));
					f2 = new IntField(other.tuples.get(j).getField(field2));
				}else{
					f1 = new StringField(tuples.get(i).getField(field1));
					f2 = new StringField(other.tuples.get(j).getField(field2));
				}
				if(f1.equals(f2)){
					//System.out.println("1 ready to be added");
					Tuple t = new Tuple(td2);
					for(int k = 0; k < td2.numFields(); k++){
						if(k < td.numFields()){
							t.setField(k, tuples.get(i).getField(k));
						}else{
							t.setField(k, other.tuples.get(j).getField(k-td.numFields()));
						}
					}
					newtuples.add(t);
					//System.out.println("1 added");
				}
			}
		}
		//System.out.println("new tuples size: " + newtuples.size());
		return new Relation(newtuples, td2);
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		Aggregator ag = new Aggregator(op, groupBy, td);
		for(int i = 0; i < tuples.size(); i++){
			ag.merge(tuples.get(i));
			//System.out.println(tuples.get(i).toString());
		}
		ArrayList<Tuple> newtuples = ag.getResults();
		return new Relation(newtuples, td);
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return this.tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		String str = "";
		for(int i = 0; i < tuples.size(); i++){
			str += tuples.get(i).toString() + "\n";
		}
		return str;
	}
}
