package simpledb.buffer;

import simpledb.server.SimpleDB;
import simpledb.file.Block;
import simpledb.buffer.*;
import static simpledb.file.Page.*;

public class NewBufferTest {
   private static BufferMgr bm = SimpleDB.bufferMgr();
   private static String filename = "tempbuffer";
   
   
   public static void testBufferMgr(String[] args) {
	  SimpleDB.init("newstudentdb");
      int avail1 = bm.available();//8
      Block blk1 = new Block(filename, 0);
      Buffer buff1 = bm.pin(blk1);
      int avail2 = bm.available();//7
      if (avail1 != avail2+1)
         System.out.println("*****BufferTest: bad available");
      Block blk2 = new Block(filename, 1);
      Buffer buff2 = bm.pin(blk2);
      int avail3 = bm.available();//6
      if (avail2 != avail3+1)
         System.out.println("*****BufferTest: bad available");
      Block blk3 = new Block(filename, 2);
      Buffer buff3 = bm.pin(blk3);
      int avail4 = bm.available();//5
      if (avail3 != avail4+1)
         System.out.println("*****BufferTest: bad available");
      bm.unpin(buff1);
      int avail5 = bm.available();//6
      if (avail4+1 != avail5)
         System.out.println("*****BufferTest: bad available");
      bm.unpin(buff3);
      int avail6 = bm.available();//7
      if (avail5 != avail6-1)
         System.out.println("*****BufferTest: bad available");
      bm.unpin(buff2);
      int avail7 = bm.available();//8
      if (avail6 != avail7-1 || avail7 != avail1)
         System.out.println("*****BufferTest: bad available");
	  Block blk4 = new Block (filename, 2);
	   buff1 = bm.pin(blk4);
	  int avail8 = bm.available();//7
	  System.out.println("avail8:"+avail8);
	  if(avail7==avail8+1)
	    System.out.println("yerlestirme basarili");
	  Block blk5 = new Block (filename , 3);
	   buff3=bm.pin(blk5);
	  int avail9 = bm.available();
	  if(avail8==avail9+1)
	    System.out.println("yerlestirme basarili");
      	  
   } 
	  
   }
   