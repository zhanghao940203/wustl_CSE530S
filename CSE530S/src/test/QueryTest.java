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

import org.junit.Before;
import org.junit.Test;

import hw1.Catalog;
import hw1.Database;
import hw1.IntField;
import hw1.Query;
import hw1.Relation;



public class QueryTest {
	
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
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");
	}
	
	@Test
	public void testSimple() {
		Query q = new Query("SELECT a1, a2 FROM A");
		Relation r = q.execute();
		
		assertTrue(r.getTuples().size() == 8);
		assertTrue(r.getDesc().getSize() == 8);
	}
	
	@Test
	public void testSelect() {
		Query q = new Query("SELECT a1, a2 FROM A WHERE a1 = 530");
		Relation r = q.execute();
		
		assert(r.getTuples().size() == 5);
		assert(r.getDesc().getSize() == 8);
	}
	
	@Test
	public void testProject() {
		Query q = new Query("SELECT a2 FROM A");
		Relation r = q.execute();
		
		assert(r.getDesc().getSize() == 4);
		assert(r.getTuples().size() == 8);
		assert(r.getDesc().getFieldName(0).equals("a2"));
	}
	
	@Test
	public void testJoin() {
		Query q = new Query("SELECT c1, c2, a1, a2 FROM test JOIN A ON test.c1 = a.a1");
		Relation r = q.execute();
		
		assert(r.getTuples().size() == 5);
		assert(r.getDesc().getSize() == 141);
	}
	
	@Test
	public void testAggregate() {
		Query q = new Query("SELECT SUM(a2) FROM A");
		Relation r = q.execute();
		
		assertTrue(r.getTuples().size() == 1);
		IntField agg = new IntField(r.getTuples().get(0).getField(0));
		assertTrue(agg.getValue() == 36);
	}
	
	@Test
	public void testGroupBy() {
		Query q = new Query("SELECT a1, SUM(a2) FROM A GROUP BY a1");
		Relation r = q.execute();
		
		assertTrue(r.getTuples().size() == 4);
	}
	
	@Test
	public void testSelectAll() {
		Query q = new Query("SELECT * FROM A");
		Relation r = q.execute();
		
		assertTrue(r.getTuples().size() == 8);
		assertTrue(r.getDesc().getSize() == 8);
	}
	
}
