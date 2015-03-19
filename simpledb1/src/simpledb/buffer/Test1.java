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
		
		
		Buffer b0 = bm.pin(new Block("student", 0));
		Buffer b1 = bm.pin(new Block("student", 1));
		Buffer b2 = bm.pin(new Block("student", 2));
		Buffer b3 = bm.pin(new Block("student", 3));
//		Buffer b4 = bm.pin(new Block("student", 4));
		
		bm.unpin(b2);
		bm.unpin(b0);
		bm.unpin(b1);
		bm.unpin(b3);
		
		System.out.println(bm.available());
		bm.unpin(b3);
		
		System.out.println(bm.available());  // this catch the bug
		
//		Buffer b4 = bm.pin(new Block("student", 4));
//		System.out.println(bm.listBuffer());
//
//		Buffer b5 = bm.pin(new Block("student", 5));
//		System.out.println(bm.listBuffer());
//
//		bm.pin(new Block("student", 1));
//		System.out.println(bm.listBuffer());
//		bm.unpin(b1);
//		System.out.println(bm.listBuffer());
//		
//		Buffer b6 = bm.pin(new Block("student", 6));
//		System.out.println(bm.listBuffer());
//		
//		b6 = bm.pin(new Block("student", 6));
//		System.out.println(bm.listBuffer());
//		
//		b6 = bm.pin(new Block("student", 6));
//		System.out.println(bm.listBuffer());
//		bm.unpin(b6);System.out.println(bm.listBuffer());
//		bm.unpin(b6);System.out.println(bm.listBuffer());
//		bm.unpin(b6);System.out.println(bm.listBuffer());
//		bm.unpin(b6);System.out.println(bm.listBuffer());

	}

}
