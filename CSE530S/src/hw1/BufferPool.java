package hw1;

import java.io.*;
import java.security.acl.Permission;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

//import hw1.Catalog.table;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public int numPages;
    
    HashMap<Integer, HeapPage> Pages = new HashMap<Integer, HeapPage>();
    HashMap<Integer, Integer> PageDirty = new HashMap<Integer, Integer>();
    HashMap<Integer, Integer> PageRLocks = new HashMap<Integer, Integer>();
    HashMap<Integer, Integer> PageWLocks = new HashMap<Integer, Integer>();
    HashMap<Integer, Set> TransactionPages = new HashMap<Integer, Set>();
    HashMap<Integer, Map> TransactionLocks = new HashMap<Integer, Map>();
    
    public BufferPool(int numPages) {
        // your code here
    	this.numPages = numPages;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param tableId the ID of the table with the requested page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public HeapPage getPage(int tid, int tableId, int pid, Permissions perm)
        throws Exception {
        // your code here
    	Catalog c = Database.getCatalog();
    	HeapFile hf = c.getDbFile(tableId);
    	HeapPage hp = hf.readPage(pid);
    	if(Pages.containsKey(pid)){
    		if(perm == Permissions.READ_ONLY){
    			HeapPage res = Pages.get(pid);
    			PageRLocks.put(pid, 1);
    			Map<Integer, Integer> TPageLock = new HashMap<Integer, Integer>();
    			TPageLock.put(pid, 0);
    			TransactionLocks.put(tid, TPageLock);
    			Set<Integer> s = new HashSet<Integer>();
    			s.add(pid);
    			TransactionPages.put(tid, s);
    			return res;
    		}else{
    			Map<Integer, Integer> m = TransactionLocks.get(tid);
    			if(PageWLocks.get(pid) == 0 || (m.containsKey(pid) && m.get(pid) == 1)){
    				HeapPage res = Pages.get(pid);
    				PageWLocks.put(pid, 1);
        			Map<Integer, Integer> TPageLock = new HashMap<Integer, Integer>();
        			TPageLock.put(pid, 1);
        			TransactionLocks.put(tid, TPageLock);
	    			Set<Integer> s = new HashSet<Integer>();
	    			s.add(pid);
	    			TransactionPages.put(tid, s);
    				return res;
    			}else{
    				Exception e = new Exception();
    				throw e;
    			}
    		}
    	}else{
    		if(Pages.keySet().size() <= this.numPages){
    			Pages.put(pid, hp);
    			PageDirty.put(pid, 0);
    			if(perm == Permissions.READ_ONLY){
	    			PageRLocks.put(pid, 1);
	    			PageWLocks.put(pid, 0);
	    			Map<Integer, Integer> TPageLock = new HashMap<Integer, Integer>();
	    			TPageLock.put(pid, 0);
	    			TransactionLocks.put(tid, TPageLock);
	    			Set<Integer> s = new HashSet<Integer>();
	    			s.add(pid);
	    			TransactionPages.put(tid, s);
    			}
    			else{
	    			PageRLocks.put(pid, 0);
	    			PageWLocks.put(pid, 1);
	    			Map<Integer, Integer> TPageLock = new HashMap<Integer, Integer>();
	    			TPageLock.put(pid, 1);
	    			TransactionLocks.put(tid, TPageLock);
	    			Set<Integer> s = new HashSet<Integer>();
	    			s.add(pid);
	    			TransactionPages.put(tid, s);
    			}
    			return hp;
    		}else{
    			evictPage();
    			Pages.put(pid, hp);
    			PageDirty.put(pid, 0);
    			if(perm == Permissions.READ_ONLY){
	    			PageRLocks.put(pid, 1);
	    			PageWLocks.put(pid, 0);
	    			Map<Integer, Integer> TPageLock = new HashMap<Integer, Integer>();
	    			TPageLock.put(pid, 0);
	    			TransactionLocks.put(tid, TPageLock);
	    			Set<Integer> s = new HashSet<Integer>();
	    			s.add(pid);
	    			TransactionPages.put(tid, s);
    			}
    			else{
	    			PageRLocks.put(pid, 0);
	    			PageWLocks.put(pid, 1);
	    			Map<Integer, Integer> TPageLock = new HashMap<Integer, Integer>();
	    			TPageLock.put(pid, 1);
	    			TransactionLocks.put(tid, TPageLock);
	    			Set<Integer> s = new HashSet<Integer>();
	    			s.add(pid);
	    			TransactionPages.put(tid, s);
    			}
    			return hp;    			
    		}
    	}
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param tableID the ID of the table containing the page to unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(int tid, int tableId, int pid) {
        // your code here
    	
    	PageRLocks.put(pid, 0);
    	PageWLocks.put(pid, 0);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public   boolean holdsLock(int tid, int tableId, int pid) {
        // your code here
    	if(PageRLocks.containsKey(pid) || PageWLocks.containsKey(pid)){
    		if(TransactionLocks.containsKey(tid)){
    			return true;
    		}
    	}
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction. If the transaction wishes to commit, write
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public   void transactionComplete(int tid, boolean commit)
        throws IOException {
        // your code here
    	Catalog c = Database.getCatalog();
    	Set<Integer> s = TransactionPages.get(tid);
    	for(int pid : s){
    		int tableId = Pages.get(pid).tableId;
    		releasePage(tid, tableId, pid);
    		if(commit){
    			HeapFile hf = c.getDbFile(tableId);
    	    	HeapPage hp = Pages.get(pid);
    	    	hf.writePage(hp);
    		}else{
    			HeapFile hf = c.getDbFile(tableId);
    			HeapPage hp = hf.readPage(pid);
    			Pages.put(pid, hp);
    		}
    		PageDirty.put(pid, 0);
    	}
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to. May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(int tid, int tableId, Tuple t)
        throws Exception {
        // your code here
    	Catalog c = Database.getCatalog();
    	HeapFile hf = c.getDbFile(tableId);
//    	Set<Integer> h =  Pages.keySet();
//    	int pid = 0;
//    	for(int p : h){
//    		if(Pages.get(p).tableId == tableId){
//    			pid = p;
//    			break;
//    		}
//    	}
    	int pid = hf.FindFreePage(t).getId();
    	System.out.println(pid);
    	//HeapPage hp = hf.readPage(pid);
    	//HeapPage hp = Pages.get(pid);
    	HeapPage hp = this.getPage(tid, tableId, pid, Permissions.READ_WRITE);
    	Map<Integer, Integer> m = TransactionLocks.get(tid);
//    	if(m.get(pid) == 1 && PageWLocks.get(pid) == 1){
//    		Pages.get(pid).addTuple(t);
    		PageDirty.put(pid, 1);
//    	}else{
//    		System.out.println("Operation blocked!");
//    	}
    		System.out.println("!");
    	System.out.println(hp.getId());
    	hp.addTuple(t);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty.
     *
     * @param tid the transaction adding the tuple.
     * @param tableId the ID of the table that contains the tuple to be deleted
     * @param t the tuple to add
     */
    public  void deleteTuple(int tid, int tableId, Tuple t)
        throws Exception {
        // your code here
//    	Set<Integer> h =  Pages.keySet();
//    	int pid = 0;
//    	for(int p : h){
//    		if(Pages.get(p).tableId == tableId){
//    			pid = p;
//    			break;
//    		}
//    	}
    	Catalog c = Database.getCatalog();
    	HeapFile hf = c.getDbFile(tableId);
    	int pid = hf.FinddeleteTuplePage(t);
//    	Map<Integer, Integer> m = TransactionLocks.get(tid);
//    	if(m.get(pid) == 1 && PageWLocks.get(pid) == 1){
//    		Pages.get(pid).deleteTuple(t);
    		PageDirty.put(pid, 1);
//    	}else{
//    		System.out.println("Operation blocked!");
//    	}
    	HeapPage hp = this.getPage(tid, tableId, pid, Permissions.READ_WRITE);
    	hp.deleteTuple(t);
    }

    private synchronized  void flushPage(int tableId, int pid) throws IOException {
        // your code here
    	Catalog c = Database.getCatalog();
    	HeapFile hf = c.getDbFile(tableId);
    	HeapPage hp = Pages.get(pid);
    	hf.writePage(hp);
    	Pages.remove(pid);
    	PageDirty.remove(pid);
    	PageRLocks.remove(pid);
    	PageWLocks.remove(pid);
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws Exception {
        // your code here
    	Set<Integer> h =  Pages.keySet();
    	for(int pid : h){
    		if(PageDirty.get(pid) == 1){
    	    	Pages.remove(pid);
    	    	PageDirty.remove(pid);
    	    	PageRLocks.remove(pid);
    	    	PageWLocks.remove(pid);
    	    	break;
    		}
    	}
    }

}
