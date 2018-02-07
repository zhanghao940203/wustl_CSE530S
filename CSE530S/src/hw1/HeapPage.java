/**
 * name: Hao Zhang
 * id: 452003
 * wustlkey: h.zhang633
 * name: Hanming Li
 * id: 451802
 * wustlkey: lihanming
 */
package hw1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class HeapPage {
	public static final int PAGE_SIZE = 4096;
	private int id;
	private byte[] header;
	private Tuple[] tuples;
	private TupleDesc td;
	private int numSlots;
	public int tableId;

	

	public HeapPage(int id, byte[] data, int tableId) throws IOException {
		this.id = id;
		this.tableId = tableId;

		this.td = Database.getCatalog().getTupleDesc(this.tableId);
		this.numSlots = getNumSlots();
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

		// allocate and read the header slots of this page
		header = new byte[getHeaderSize()];
		for (int i=0; i<header.length; i++)
			header[i] = dis.readByte();

		try{
			// allocate and read the actual records of this page
			tuples = new Tuple[numSlots];
			for (int i=0; i<tuples.length; i++)
				tuples[i] = readNextTuple(dis,i);
		}catch(NoSuchElementException e){
			e.printStackTrace();
		}
		dis.close();
	}

	public int getId() {
		//your code here
		return this.id;
	}

	/**
	 * Computes and returns the total number of slots that are on this page (occupied or not).
	 * Must take the header into account!
	 * @return number of slots on this page
	 */
	public int getNumSlots() {
		//your code here
		int num = (int)Math.floor(PAGE_SIZE*8)/(td.getSize()*8+1);
		return num;
	}

	/**
	 * Computes the size of the header. Headers must be a whole number of bytes (no partial bytes)
	 * @return size of header in bytes
	 */
	private int getHeaderSize() {        
		//your code here
		int size;
		if(this.getNumSlots()%8 != 0){
			size =this.getNumSlots()/8+1;
		}else{
			size = this.getNumSlots()/8;}
		return size;
	}

	/**
	 * Checks to see if a slot is occupied or not by checking the header
	 * @param s the slot to test
	 * @return true if occupied
	 */
	public boolean slotOccupied(int s) {
		//your code here
		int i = s/8;
		int j = s%8;
		byte b = header[i];
		int bit = (byte) ((b >> j) & 0x1);
		if(bit == 1){
			return true;
		}
		return false;
	}

	/**
	 * Sets the occupied status of a slot by modifying the header
	 * @param s the slot to modify
	 * @param value its occupied status
	 */
	public void setSlotOccupied(int s, boolean value) {
		//your code here
		
		int i = s/8;
		int a = 0x01;
		int j = s%8;
		//System.out.println(slotOccupied(s));
			if(value == true && slotOccupied(s) == false){
				//header[i] = (byte) (header[i] - ((a << j) & 0x1));
				header[i] = (byte) (header[i] + (a << j));
			}
			if(value == false && slotOccupied(s) == true){
				//header[i] = (byte) (header[i] + ((a << j) & 0x1));
				header[i] = (byte) (header[i] - (a << j));
			}
		//System.out.println(slotOccupied(s));
	}
	
	/**
	 * Adds the given tuple in the next available slot. Throws an exception if no empty slots are available.
	 * Also throws an exception if the given tuple does not have the same structure as the tuples within the page.
	 * @param t the tuple to be added.
	 * @throws Exception
	 */
	public void addTuple(Tuple t) throws Exception {
		//your code here
		//System.out.println(this.slotOccupied(1));
		if(t.getDesc() != this.td){
			throw new Exception();
		}
		//System.out.println(tuples.length);
		for(int i = 0; i < tuples.length; i++){
			if(slotOccupied(i) == false){
				tuples[i] = t;
				tuples[i].setId(i);
				tuples[i].setPid(this.getId());
				setSlotOccupied(i, true);
				break;
			}
			else if(i == tuples.length-1 && slotOccupied(i) == true){
				throw new Exception();
			}
		}
		//System.out.println(this.slotOccupied(1));
		
	}

	/**
	 * Removes the given Tuple from the page. If the page id from the tuple does not match this page, throw
	 * an exception. If the tuple slot is already empty, throw an exception
	 * @param t the tuple to be deleted
	 * @throws Exception
	 */
	public void deleteTuple(Tuple t) throws Exception {
		//your code here
		//System.out.println(this.slotOccupied(0));
		//System.out.println(this.slotOccupied(1));
		if(t.getPid() != this.id){
			//System.out.print("1");
			throw new Exception();
			
		}
		
		for(int i = 0; i < tuples.length; i++){
			if(tuples[i].toString().equals(t.toString())){
				//System.out.println(tuples[i].toString());
				//System.out.println(t.toString());
//				System.out.println(tuples[i].getField(1));
				if(slotOccupied(i) == false){
					//System.out.print("2");
					throw new Exception();
				}
				tuples[i] = null;
//				for(int j = 0; j < tuples[i].getDesc().numFields(); j++){
//					tuples[i].setField(j, null);
//				}
//				System.out.println(tuples[i].getField(1));
				//System.out.println(i);
				
				setSlotOccupied(i, false);
				//System.out.println(this.slotOccupied(0));
				//System.out.println(this.slotOccupied(1));
				break;
			}
			
		}
		//System.out.println(this.slotOccupied(0));
//		System.out.println(this.slotOccupied(1));
	}
	
	/**
     * Suck up tuples from the source file.
     */
	private Tuple readNextTuple(DataInputStream dis, int slotId) {
		// if associated bit is not set, read forward to the next tuple, and
		// return null.
		if (!slotOccupied(slotId)) {
			for (int i=0; i<td.getSize(); i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty tuple");
				}
			}
			return null;
		}

		// read fields in the tuple
		Tuple t = new Tuple(td);
		t.setPid(this.id);
		t.setId(slotId);

		for (int j=0; j<td.numFields(); j++) {
			if(td.getType(j) == Type.INT) {
				byte[] field = new byte[4];
				try {
					dis.read(field);
					t.setField(j, field);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				//byte[] field = new byte[132];
				byte[] field = new byte[129];
				try {
					dis.read(field);
					t.setField(j, field);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}


		return t;
	}

	/**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
	 *
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     */
	public byte[] getPageData() {
		int len = HeapFile.PAGE_SIZE;
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// create the header of the page
		for (int i=0; i<header.length; i++) {
			try {
				dos.writeByte(header[i]);
			} catch (IOException e) {
				// this really shouldn't happen
				e.printStackTrace();
			}
		}

		// create the tuples
		for (int i=0; i<tuples.length; i++) {
			//System.out.println(i);
			//System.out.println(slotOccupied(i));
			// empty slot
			if (!slotOccupied(i)) {
				for (int j=0; j<td.getSize(); j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			for (int j=0; j<td.numFields(); j++) {
				byte[] f = tuples[i].getField(j);
				try {
					dos.write(f);

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// padding
		int zerolen = HeapFile.PAGE_SIZE - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
		byte[] zeroes = new byte[zerolen];
		try {
			dos.write(zeroes, 0, zerolen);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos.toByteArray();
	}

	/**
	 * Returns an iterator that can be used to access all tuples on this page. 
	 * @return
	 */
	public Iterator<Tuple> iterator() {
		//your code here
		ArrayList<Tuple> l = new ArrayList<Tuple>();
		
		//System.out.println(this.slotOccupied(1));
		for(int i = 0; i < tuples.length; i++){
			//int n=0;
			//System.out.println(this.slotOccupied(i));
			if(this.slotOccupied(i) == false){
				continue;
			}
//			for(int j = 0; j < tuples[i].getDesc().numFields(); j++){
//				if(tuples[i].getField(j)==null){
//					n++;
//				}
//			}
//			if(n==tuples[i].getDesc().numFields()){
//				continue;
//			}
			//System.out.println("the tuple "+i+" is "+tuples[i].getField(0));
			l.add(tuples[i]);
		}
		Iterator<Tuple> iter = l.iterator();
		//System.out.println("this page has tuples " + l.size());
		return iter;
	}
}
