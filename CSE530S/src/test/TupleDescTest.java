/**
 * name: Hao Zhang
 * id: 452003
 * wustlkey: h.zhang633
 * name: Hanming Li
 * id: 451802
 * wustlkey: lihanming
 */
package test;

import static org.junit.Assert.*;

import java.util.NoSuchElementException;

import org.junit.Test;

import hw1.TupleDesc;
import hw1.Type;

public class TupleDescTest {
	
	private static final String alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
	

	private Type[] randomTypes(int n) {
		Type[] t = new Type[n];
		for(int i = 0; i < n; i++) {
			if(Math.random() > .5) {
				t[i] = Type.INT;
			}
			else {
				t[i] = Type.STRING;
			}
		}
		
		return t;
	}
	
	private String[] randomColumns(int n) {
		String[] c = new String[n];
		for(int i = 0; i < n; i++) {
			int l = (int)(Math.random() * 12 + 2);
			String s = "";
			for (int j = 0; j < l; j++) {
				s += alphabet.charAt((int)(Math.random() * 36));
			}
			c[i] = s;
		}
		return c;
	}
	
	@Test
	public void testGetType() {
		for(int i = 0; i < 10; i++) {
			int size = (int)(Math.random() * 15 + 1);
			Type[] t = randomTypes(size);
			String[] c = randomColumns(size);
			TupleDesc td = new TupleDesc(t, c);
			for(int j = 0; j < size; j++) {
				assertTrue(td.getType(j) == t[j]);
			}
		}
		
	}
	
	@Test
	public void testNameToId() {
		for(int i = 0; i < 10; i++) {
			int size = (int)(Math.random() * 15 + 1);
			Type[] t = randomTypes(size);
			String[] c = randomColumns(size);
			TupleDesc td = new TupleDesc(t, c);
			for(int j = 0; j < size; j++) {
				assertTrue(td.nameToId(c[j]) == j);
			}
		}
		
		try {
			int size = (int)(Math.random() * 15 + 1);
			Type[] t = randomTypes(size);
			String[] c = randomColumns(size);
			TupleDesc td = new TupleDesc(t, c);
			td.nameToId("");
			fail("found column name that does not exist");
		} catch(NoSuchElementException e) {
			
		}
	}
	
	@Test
	public void testGetSize() {
		Type[] t = {Type.INT, Type.INT, Type.INT};
		String[] c = {"", "", ""};
		TupleDesc td = new TupleDesc(t, c);
		assertTrue(td.getSize() == 12);
	}
	
	@Test
	public void testEquals() {
		TupleDesc td1 = new TupleDesc(new Type[]{Type.INT}, new String[]{""});
		TupleDesc td2 = new TupleDesc(new Type[]{Type.INT}, new String[]{""});
		TupleDesc td3 = new TupleDesc(new Type[]{Type.INT, Type.STRING}, new String[]{"", ""});
		
		assertTrue(td1.equals(td2));
		assertTrue(td2.equals(td1));
		assertTrue(td1.equals(td1));
		assertTrue(td2.equals(td2));
		
		assertFalse(td1.equals(td3));
		assertFalse(td2.equals(td3));
		assertFalse(td3.equals(td1));
	}

}
