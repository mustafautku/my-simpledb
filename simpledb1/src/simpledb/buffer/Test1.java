package simpledb.buffer;

import simpledb.file.Block;
import simpledb.server.SimpleDB;

public class Test1 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Init1Table.initData("db1table");  // contains "student" table
		BufferMgr bm2=SimpleDB.bufferMgr();
		BufferMgr bm=new BufferMgr(4);  // initialize a new "smaller" buffer pool. 
		System.out.println(bm.listBuffer());
		
		Buffer b0 = bm.pin(new Block("student", 0));
		Buffer b1 = bm.pin(new Block("student", 1));
		Buffer b2 = bm.pin(new Block("student", 2));
		Buffer b3 = bm.pin(new Block("student", 3));
		System.out.println(bm.listBuffer());

		bm.unpin(b2);
		bm.unpin(b0);
		bm.unpin(b1);
		bm.unpin(b3);  // unpinned list : (LRU)b2, b0, b1, b3
		
		System.out.println(bm.listBuffer());
		System.out.println(bm.available());
//		bm.unpin(b3);    // decrease to negative..but, IGNORE THIS BUG: UPPER SYSTEM MODULES uses Buffer appropriately.
//		System.out.println(bm.listBuffer());
//		System.out.println(bm.available());
		
		bm.pin(new Block("student", 4));  // unpinned list : (LRU)b0, b1, b3
		System.out.println(bm.listBuffer());

		bm.pin(new Block("student", 5));  // unpinned list : (LRU)b1, b3
		System.out.println(bm.listBuffer());

		bm.pin(new Block("student", 1));   // unpinned list : (LRU)b3
		System.out.println(bm.listBuffer());
		bm.unpin(b1);                      // unpinned list : (LRU)b3, b1
		System.out.println(bm.listBuffer());
		
		bm.pin(new Block("student", 6));  // PLACES INTO b3.   // unpinned list : (LRU)b1
		System.out.println(bm.listBuffer());
		
		bm.pin(new Block("student", 6));
		System.out.println(bm.listBuffer());
		
		bm.pin(new Block("student", 6));
		System.out.println(bm.listBuffer());
		bm.unpin(b3);System.out.println(bm.listBuffer());
		bm.unpin(b3);System.out.println(bm.listBuffer());
		bm.unpin(b3);System.out.println(bm.listBuffer()); // unpinned list : (LRU)b1, b3, 
		
		bm.pin(new Block("student", 1));  // unpinned list : (LRU)b3, findExsistingBuffer finds b1. should remove from unpinned list.
		bm.pin(new Block("student", 10)); // unpinned list : boþ
		System.out.println(bm.listBuffer());
		
		//unpin all remainings
		bm.unpin(b0);    // unpinned list : (LRU)b0
		bm.unpin(b1);    // unpinned list : (LRU)b0,b1
		bm.unpin(b2);    // unpinned list : (LRU)b0,b1,b2
		bm.unpin(b3);    // unpinned list : (LRU)b0,b1,b2,b3
		System.out.println(bm.listBuffer());  // All are shown as unpinned..

	}

}
