package simpledb.file;

import simpledb.server.SimpleDB;

/*
 * Test for get/setString delimiter-type implementation.
 */

public class Test1 {
	private static String filename = "file1"; // DO NOT NAME filename as "tempfile" 
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SimpleDB.init("test1");
		Block b = new Block(filename, 0);
		Page p1 = new Page();
		Page p2 = new Page();
		p1.setString(0, "utku");  //5 chars, each of which 2B.
		p1.setString(10, "kalay"); // starts at 10.Byte
		p1.write(b);
		p2.read(b);
		System.out.println(p2.getString(0) + " " + p2.getString(10));
		
		b = new Block(filename, 100);
		FileMgr fm = SimpleDB.fileMgr();
		int lastblock = fm.size(filename) - 1;
		System.out.println(lastblock);
		
		b = new Block(filename, 10);
		p2.write(b);
		System.out.println("filesize="+fm.size(filename));
		
		b = new Block(filename, 5);
		p2.read(b);
		System.out.println(p2.getString(0) + " " + p2.getString(2));  //read '/0' char.
		
	}

}
