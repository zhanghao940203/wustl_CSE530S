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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import hw1.AggregateOperator;
import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.IntField;
import hw1.Relation;
import hw1.RelationalOperator;
import hw1.TupleDesc;

public class RelationTest {

	private HeapFile testhf;
	private TupleDesc testtd;
	private HeapFile ahf;
	private TupleDesc atd;
	private Catalog c;

	@Before
	public void setup() {
		
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		int tableId = c.getTableId("test");
		testtd = c.getTupleDesc(tableId);
		testhf = c.getDbFile(tableId);
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");
		
		tableId = c.getTableId("A");
		atd = c.getTupleDesc(tableId);
		ahf = c.getDbFile(tableId);
	}
	
	@Test
	public void testSelect() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.select(0, RelationalOperator.EQ, new IntField(530));
		
		assert(ar.getTuples().size() == 5);
		assert(ar.getDesc().equals(atd));
	}
	
	@Test
	public void testProject() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		assert(ar.getDesc().getSize() == 4);
		assert(ar.getTuples().size() == 8);
		assert(ar.getDesc().getFieldName(0).equals("a2"));
	}
	
	@Test
	public void testJoin() {
		Relation tr = new Relation(testhf.getAllTuples(), testtd);
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		tr = tr.join(ar, 0, 0);
		
		assert(tr.getTuples().size() == 5);
		assert(tr.getDesc().getSize() == 141);
	}
	
	@Test
	public void testRename() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		
		ArrayList<Integer> f = new ArrayList<Integer>();
		ArrayList<String> n = new ArrayList<String>();
		
		f.add(0);
		n.add("b1");
		
		ar = ar.rename(f, n);
		
		assertTrue(ar.getTuples().size() == 8);
		assertTrue(ar.getDesc().getFieldName(0).equals("b1"));
		assertTrue(ar.getDesc().getFieldName(1).equals("a2"));
		assertTrue(ar.getDesc().getSize() == 8);
		
	}
	
	@Test
	public void testAggregate() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.SUM, false);
		
		assertTrue(ar.getTuples().size() == 1);
		IntField agg = new IntField(ar.getTuples().get(0).getField(0));
		assertTrue(agg.getValue() == 36);
	}
	
	@Test
	public void testGroupBy() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.aggregate(AggregateOperator.SUM, true);
		
		assertTrue(ar.getTuples().size() == 4);
	}

}
