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

import hw1.BufferPool;

/*
 * Student 1 name:
 * Student 2 name:
 * Date: 
 */

/** Database is a class that initializes a static
    variable used by the database system (the catalog)

    Provides a set of methods that can be used to access these variables
    from anywhere.
 */

public class Database {
	private static Database _instance = new Database();
	private final Catalog _catalog;
	private BufferPool _bufferpool; 


	private Database() {
		_catalog = new Catalog();
		_bufferpool = new BufferPool(BufferPool.DEFAULT_PAGES);
	}

	/** Return the catalog of the static Database instance*/
	public static Catalog getCatalog() {
		return _instance._catalog;
	}

	/** Return the buffer pool of the static Database instance*/
	public static BufferPool getBufferPool() {
		return _instance._bufferpool;
	}

	/** Method used for testing -- create a new instance of the
    buffer pool and return it
	 */
	public static BufferPool resetBufferPool(int pages) {
		_instance._bufferpool = new BufferPool(pages);
		return _instance._bufferpool;
	}

	//reset the database, used for unit tests only.
	public static void reset() {
		_instance = new Database();
	}

}
