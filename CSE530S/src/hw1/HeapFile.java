/**
 * name: Hao Zhang
 * id: 452003
 * wustlkey: h.zhang633
 * name: Hanming Li
 * id: 451802
 * wustlkey: lihanming
 */
package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	File F;
	TupleDesc tb;
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		//your code here
		this.F = f;
		this.tb = type;
	}
	
	public File getFile() {
		//your code here
		return this.F;
	}
	
	public TupleDesc getTupleDesc() {
		//your code here
		return this.tb;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 * @throws IOException 
	 */
	public HeapPage readPage(int id){
		//your code here
		RandomAccessFile rf;
		try {
			rf = new RandomAccessFile(this.F, "rw");
			rf.seek(PAGE_SIZE * id);
			byte[] b = new byte[PAGE_SIZE];
			rf.read(b);
			rf.close();
			HeapPage hp = new HeapPage(id, b, this.getId());
			
			return hp;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return this.F.getAbsoluteFile().hashCode();
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 * @throws IOException 
	 */
	public void writePage(HeapPage p) {
		//your code here
		RandomAccessFile rf;
		try {
			rf = new RandomAccessFile(this.F, "rw");
			int id = p.getId();
			rf.seek(PAGE_SIZE * id);
			byte[] b = p.getPageData();
			rf.write(b);
			rf.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 */
	public HeapPage addTuple(Tuple t) {
		//your code here
		try {
			int o = 0;
			HeapPage hp = null;
			for(int i = 0; i < this.getNumPages(); i++){
				hp = this.readPage(i);
				for(int j = 0; j < hp.getNumSlots(); j++){
					if(hp.slotOccupied(j) == false){
						o = 1;
						//System.out.println("!!!!!!!!!!!!!");
						break;
					}
				}
				if(o == 1){
					//System.out.println(hp.getId());
					//System.out.println(t.getField(0));
					//System.out.println(hp.slotOccupied(1));
					hp.addTuple(t);
					//System.out.println(hp.slotOccupied(1));
					break;
				}
			}
			if(o == 0){
				//System.out.println(3);
				hp = new HeapPage(this.getNumPages() + 1, new byte[PAGE_SIZE], this.getId());
				hp.addTuple(t);
			}
			this.writePage(hp);
			return hp;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public HeapPage FindFreePage(Tuple t) {
		//your code here
		try {
			int o = 0;
			HeapPage hp = null;
			for(int i = 0; i < this.getNumPages(); i++){
				hp = this.readPage(i);
				for(int j = 0; j < hp.getNumSlots(); j++){
					if(hp.slotOccupied(j) == false){
						o = 1;
						//System.out.println("!!!!!!!!!!!!!");
						break;
					}
				}
			}
			if(o == 0){
				//System.out.println(3);
				hp = new HeapPage(this.getNumPages() + 1, new byte[PAGE_SIZE], this.getId());
				this.writePage(hp);
			}
			return hp;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t){
		//your code here
		try {
			int pid = t.getPid();
			HeapPage hp = this.readPage(pid);
			hp.deleteTuple(t);
			this.writePage(hp);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public int FinddeleteTuplePage(Tuple t){
		//your code here
		try {
			int pid = t.getPid();
			HeapPage hp = this.readPage(pid);
			return hp.getId();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> l = new ArrayList<Tuple>();
		int length = this.getNumPages();
		//System.out.println(length);
		for(int i = 0; i < length; i++){
			HeapPage hp = this.readPage(i);
			Iterator<Tuple> iter = hp.iterator();
			int j=0;
			while(iter.hasNext()){
				l.add(iter.next());
				j++;
				//System.out.println("ADD "+j);
			}
		}
		//System.out.println("all tupe size "+l.size());
		return l;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		int l = 0;
		
		try {
			RandomAccessFile rf = new RandomAccessFile(this.F, "rw");
			l = (int) (rf.length()/PAGE_SIZE);
			rf.close();
			return l;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return l;
	}
}
