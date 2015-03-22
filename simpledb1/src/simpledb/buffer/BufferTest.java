package simpledb.buffer;


import simpledb.file.Block;
import simpledb.server.SimpleDB;

public class BufferTest {

   public static void main(String[] args) {
      test2();
   }

   private static void test1() {
      int BUFFER_SIZE = 2;

      SimpleDB.initFileMgr("dbfiles");

      BufferMgr bufmgr = new BufferMgr(BUFFER_SIZE);

      Block b1 = new Block("exampleblockfile", 1);
      Block b2 = new Block("exampleblockfile", 2);
      Block b3 = new Block("exampleblockfile", 3);
      Block b4 = new Block("exampleblockfile", 4);
      Block b5 = new Block("exampleblockfile", 5);
      Block b6 = new Block("exampleblockfile", 6);

      System.out.println("Starting");
      System.out.println(bufmgr.toString());

      bufmgr.pin(b1);
      System.out.println(bufmgr.toString());

      bufmgr.unpin(0);
      System.out.println(bufmgr.toString());

      bufmgr.pin(b2);
      System.out.println(bufmgr.toString());

      bufmgr.pin(b3);
      System.out.println(bufmgr.toString());
   }

   private static void test2() {
      int BUFFER_SIZE = 4;

      SimpleDB.initFileMgr("dbfiles");

      BufferMgr bufmgr = new BufferMgr(BUFFER_SIZE);

      Block b1 = new Block("exampleblockfile", 1);
      Block b2 = new Block("exampleblockfile", 2);
      Block b3 = new Block("exampleblockfile", 3);
      Block b4 = new Block("exampleblockfile", 4);
      Block b5 = new Block("exampleblockfile", 5);
      Block b6 = new Block("exampleblockfile", 6);
      Block b7 = new Block("exampleblockfile", 7);
      Block b8 = new Block("exampleblockfile", 8);

      System.out.println("Starting. Initial Status:");
      System.out.println(bufmgr.toString());

      bufmgr.pin(b1);

      bufmgr.unpin(0);

      bufmgr.pin(b2);

      bufmgr.pin(b3);

      bufmgr.pin(b4);

      bufmgr.unpin(3);

      bufmgr.unpin(1);

      bufmgr.unpin(2);

      bufmgr.pin(b5);

      bufmgr.pin(b6);

      bufmgr.pin(b7);

      bufmgr.pin(b8);
   }

}
