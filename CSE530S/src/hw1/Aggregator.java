/**
 * name: Hao Zhang
 * id: 452003
 * wustlkey: h.zhang633
 * name: Hanming Li
 * id: 451802
 * wustlkey: lihanming
 */
package hw1;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	AggregateOperator o;
	boolean groupBy;
	TupleDesc td;
	int avg;
	int count;
	int max;
	int min;
	int sum;
	
	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
	ArrayList<Tuple> temp = new ArrayList<Tuple>();
	Map<String, Integer> msum = new HashMap<String, Integer>();
	Map<String, Integer> mavg = new HashMap<String, Integer>();
	Map<String, Integer> mc = new HashMap<String, Integer>();
	Map<String, Integer> mmax = new HashMap<String, Integer>();
	Map<String, Integer> mmin = new HashMap<String, Integer>();
	
	Map<Integer, Integer> msum2 = new HashMap<Integer, Integer>();
	Map<Integer, Integer> mavg2 = new HashMap<Integer, Integer>();
	Map<Integer, Integer> mc2 = new HashMap<Integer, Integer>();
	Map<Integer, Integer> mmax2 = new HashMap<Integer, Integer>();
	Map<Integer, Integer> mmin2 = new HashMap<Integer, Integer>();
	
	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		this.o = o;
		this.groupBy = groupBy;
		this.td = td;
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		//your code here
		
		temp.add(t);
		if(!groupBy){
			if(td.getType(0) == Type.STRING){
				if(o == AggregateOperator.COUNT){
					count++;
				}
				else{
					System.out.println("Cannot");
				}
			}
			else{
				if(o == AggregateOperator.AVG || o == AggregateOperator.SUM || o == AggregateOperator.COUNT){
					byte[] v = t.getField(0);
					int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
					//System.out.println(n);
					sum += n;
					count++;
				}
				else if(o == AggregateOperator.MAX){
					byte[] v = t.getField(0);
					int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
					max = Math.max(max, n);
				}
				else if(o == AggregateOperator.MIN){
					byte[] v = t.getField(0);
					int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
					min = Math.min(max, n);
				}
			}
		}
		else{
			if(td.getType(1) == Type.STRING){
				if(o == AggregateOperator.COUNT){
					if(td.getType(0) == Type.INT){
						byte[] v = t.getField(0);
						int str = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
						if(!mc2.containsKey(str)){
							mc2.put(str, 1);
						}
						else{
							mc2.put(str, mc2.get(t.getField(0))+1);
						}
					}
					else{
						byte[] b = t.getField(0);
						String str = new String(b);
						if(!mc.containsKey(str)){
							mc.put(str, 1);
						}
						else{
							mc.put(str, mc.get(t.getField(0))+1);
						}
					}
				}
				else{
					System.out.println("Cannot");
				}
			}
			else{
				if(td.getType(0) == Type.INT){
					if(o == AggregateOperator.AVG || o == AggregateOperator.SUM || o == AggregateOperator.COUNT){
						byte[] b = t.getField(0);
						int str = (int)(((b[0] & 0xFF)<<24) | ((b[1] & 0xFF)<<16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF));
						if(!mc2.containsKey(str)){
							mc2.put(str, 1);
							byte[] v = t.getField(1);
							int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
							msum2.put(str, n);
							mavg2.put(str, n);
							//System.out.println("1");
						}else{
							mc2.put(str, mc2.get(str)+1);
							byte[] v = t.getField(1);
							int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
							msum2.put(str, msum2.get(str)+n);
							mavg2.put(str, msum2.get(str)/mc2.get(str));
						}
					}
					else if(o == AggregateOperator.MAX){
						byte[] b = t.getField(0);
						int str = (int)(((b[0] & 0xFF)<<24) | ((b[1] & 0xFF)<<16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF));
						if(!mmax2.containsKey(str)){
							byte[] v = t.getField(1);
							int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
							mmax2.put(str, n);
						}else{
							byte[] v = t.getField(1);
							int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
							mmax2.put(str, Math.max(mmax2.get(str), n));
						}
					}
					else if(o == AggregateOperator.MIN){
						byte[] b = t.getField(0);
						int str = (int)(((b[0] & 0xFF)<<24) | ((b[1] & 0xFF)<<16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF));
						if(!mmin2.containsKey(str)){
							byte[] v = t.getField(1);
							int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
							mmin2.put(str, n);
						}
						else{
							byte[] v = t.getField(1);
							int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
							mmin2.put(str, Math.min(mmin2.get(str), n));
						}
				}else{
					if(o == AggregateOperator.AVG || o == AggregateOperator.SUM || o == AggregateOperator.COUNT){
						byte[] b = t.getField(0);
						String str = new String(b);
						if(!mc.containsKey(str)){
							mc.put(str, 1);
							byte[] v = t.getField(1);
							int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
							msum.put(str, n);
							mavg.put(str, n);
							//System.out.println("1");
						}else{
							mc.put(str, mc.get(str)+1);
							byte[] v = t.getField(1);
							int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
							msum.put(str, msum.get(str)+n);
							mavg.put(str, msum.get(str)/mc.get(str));
						}
					}
					else if(o == AggregateOperator.MAX){
						byte[] b = t.getField(0);
						String str = new String(b);
						if(!mmax.containsKey(str)){
							byte[] v = t.getField(1);
							int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
							mmax.put(str, n);
						}else{
							byte[] v = t.getField(1);
							int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
							mmax.put(str, Math.max(mmax.get(str), n));
						}
					}
					else if(o == AggregateOperator.MIN){
						byte[] b = t.getField(0);
						String str = new String(b);
						if(!mmin.containsKey(str)){
							byte[] v = t.getField(1);
							int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
							mmin.put(str, n);
						}
						else{
							byte[] v = t.getField(1);
							int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
							mmin.put(str, Math.min(mmin.get(str), n));
						}
					}
				}
			}
			}
		}
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		if(!groupBy){
			if(o == AggregateOperator.MAX){
	//			for(int i = 0; i < temp.size(); i++){
	//				byte[] v = temp.get(i).getField(0);
	//				int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
	//				if(n == max){
	//					tuples.add(temp.get(i));
	//				}
	//			}
				Tuple t = new Tuple(td);
				byte[] bytes = ByteBuffer.allocate(4).putInt(max).array();
				t.setField(0, bytes);
				tuples.add(t);
			}
			if(o == AggregateOperator.MIN){
	//			for(int i = 0; i < temp.size(); i++){
	//				byte[] v = temp.get(i).getField(0);
	//				int n = (int)(((v[0] & 0xFF)<<24) | ((v[1] & 0xFF)<<16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF));
	//				if(n == min){
	//					tuples.add(temp.get(i));
	//				}
	//			}
				Tuple t = new Tuple(td);
				byte[] bytes = ByteBuffer.allocate(4).putInt(min).array();
				t.setField(0, bytes);
				tuples.add(t);
			}
			if(o == AggregateOperator.SUM){
				Tuple t = new Tuple(td);
				byte[] bytes = ByteBuffer.allocate(4).putInt(sum).array();
				t.setField(0, bytes);
				tuples.add(t);
				//System.out.println(sum);
			}
			if(o == AggregateOperator.COUNT){
				Tuple t = new Tuple(td);
				byte[] bytes = ByteBuffer.allocate(4).putInt(count).array();
				t.setField(0, bytes);
				tuples.add(t);
			}
			if(o == AggregateOperator.AVG){
				avg = (int)sum/count;
				Tuple t = new Tuple(td);
				byte[] bytes = ByteBuffer.allocate(4).putInt(avg).array();
				t.setField(0, bytes);
				tuples.add(t);
			}
		}else{
			if(td.getType(0) == Type.INT){
				if(o == AggregateOperator.MAX){
					Iterator iter = mmax2.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						int key = (int) entry.getKey();
						int val = (int) entry.getValue();
						Tuple t = new Tuple(td);
						byte[] bytes = ByteBuffer.allocate(4).putInt(val).array();
						byte[] kbytes = ByteBuffer.allocate(4).putInt(key).array();
						t.setField(0, kbytes);
						t.setField(1, bytes);
						tuples.add(t);
					}
				}
				if(o == AggregateOperator.MIN){
					Iterator iter = mmin2.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						int key = (int) entry.getKey();
						int val = (int) entry.getValue();
						Tuple t = new Tuple(td);
						byte[] bytes = ByteBuffer.allocate(4).putInt(val).array();
						byte[] kbytes = ByteBuffer.allocate(4).putInt(key).array();
						t.setField(0, kbytes);
						t.setField(1, bytes);
						tuples.add(t);
					}
				}
				if(o == AggregateOperator.SUM){
					Iterator iter = msum2.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						int key = (int) entry.getKey();
						int val = (int) entry.getValue();
						Tuple t = new Tuple(td);
						byte[] bytes = ByteBuffer.allocate(4).putInt(val).array();
						byte[] kbytes = ByteBuffer.allocate(4).putInt(key).array();
						t.setField(0, kbytes);
						t.setField(1, bytes);
						tuples.add(t);
	//					System.out.println("1");
					}
				}
				if(o == AggregateOperator.COUNT){
					Iterator iter = mc2.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						int key = (int) entry.getKey();
						int val = (int) entry.getValue();
						Tuple t = new Tuple(td);
						byte[] bytes = ByteBuffer.allocate(4).putInt(val).array();
						byte[] kbytes = ByteBuffer.allocate(4).putInt(key).array();
						t.setField(0, kbytes);
						t.setField(1, bytes);
						tuples.add(t);
					}
				}
				if(o == AggregateOperator.AVG){
					Iterator iter = mavg2.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						int key = (int) entry.getKey();
						int val = (int) entry.getValue();
						Tuple t = new Tuple(td);
						byte[] bytes = ByteBuffer.allocate(4).putInt(val).array();
						byte[] kbytes = ByteBuffer.allocate(4).putInt(key).array();
						t.setField(0, kbytes);
						t.setField(1, bytes);
						tuples.add(t);
					}
				}
			}
			else{
				if(o == AggregateOperator.MAX){
					Iterator iter = mmax.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						String key = (String) entry.getKey();
						int val = (int) entry.getValue();
						Tuple t = new Tuple(td);
						byte[] bytes = ByteBuffer.allocate(4).putInt(val).array();
						t.setField(0, key.getBytes());
						t.setField(1, bytes);
						tuples.add(t);
					}
				}
				if(o == AggregateOperator.MIN){
					Iterator iter = mmin.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						String key = (String) entry.getKey();
						int val = (int) entry.getValue();
						Tuple t = new Tuple(td);
						byte[] bytes = ByteBuffer.allocate(4).putInt(val).array();
						t.setField(0, key.getBytes());
						t.setField(1, bytes);
						tuples.add(t);
					}
				}
				if(o == AggregateOperator.SUM){
					Iterator iter = msum.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						String key = (String) entry.getKey();
						int val = (int) entry.getValue();
						Tuple t = new Tuple(td);
						byte[] bytes = ByteBuffer.allocate(4).putInt(val).array();
						t.setField(0, key.getBytes());
						t.setField(1, bytes);
						tuples.add(t);
	//					System.out.println("1");
					}
				}
				if(o == AggregateOperator.COUNT){
					Iterator iter = mc.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						String key = (String) entry.getKey();
						int val = (int) entry.getValue();
						Tuple t = new Tuple(td);
						byte[] bytes = ByteBuffer.allocate(4).putInt(val).array();
						t.setField(0, key.getBytes());
						t.setField(1, bytes);
						tuples.add(t);
					}
				}
				if(o == AggregateOperator.AVG){
					Iterator iter = mavg.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						String key = (String) entry.getKey();
						int val = (int) entry.getValue();
						Tuple t = new Tuple(td);
						byte[] bytes = ByteBuffer.allocate(4).putInt(val).array();
						t.setField(0, key.getBytes());
						t.setField(1, bytes);
						tuples.add(t);
					}
				}
			}
		}
		return tuples;
	}
}
