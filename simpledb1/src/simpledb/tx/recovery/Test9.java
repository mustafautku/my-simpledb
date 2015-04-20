package simpledb.tx.recovery;

import simpledb.tx.*;
import simpledb.file.*;
import simpledb.server.SimpleDB;

/*
 * Bu test case'de önceki gibi 3 hareketin eþ zamanlý çalýþmasý organize edilmiþtir. 
 * Bu test TX4 un **AKTIF** CKPT atmasýný test ediyor. 
 * Bunun için Transaction.javada constructorda çaðýrýlan  dealWithCheckPoint(txnum); fonksiyonunu UNcomment etmeniz yeterli...
 * N (CKPT threshold)=4 olarak set edin.
 * Bu test sonuda olmasý gereken log dosyasý aþaðýda listelenmiþtir. 
 */


public class Test9 {
   private static String result = "";
   
   public static void main(String[] args) {
	   System.out.println("BEGIN RECOVERY PACKAGE TEST: ## 7 ");
      SimpleDB.init("testrecovery");

      // TX 2:  R(1), R(2)
      Test9A t2 = new Test9A();
      Thread th2 = new Thread(t2);
      th2.start();
      
      // TX 3:  W(2), R(1)
      Test9B t3 = new Test9B();
      Thread th3 = new Thread(t3);
      th3.start();
      
      //TX 4:   W(1), R(2)
      Test9C t4 = new Test9C();
      Thread th4 = new Thread(t4);
      th4.start();
      
      // concurrent execution
      try {
         th2.join();
         th3.join();
         th4.join();
      }
      catch (InterruptedException e) {}
     
      // TX 5 is only for debug.
      Transaction tx = new Transaction();
      tx.commit();
      tx.listLog();
      
      //Çalýþma planý aþaðýdaki gibi olmali. Çünkü harektler içinde beklemeler ile (Thread.sleep(1500);  gibi)  bu plana zorlandý. 
      // Plan T3,T2, T1 ve hepsi de rollback yapýyor.
      String correctResult =
    		  "Tx 2: read 1 start\n" +
    			         "Tx 2: read 1 end\n" +
    			         "Tx 3: write 2 start\n" +
    			         "Tx 3: write 2 end\n" +
    			         "Tx 4: write 1 start\n" +
    			         "Tx 2: read 2 start\n" +
    			         "Tx 3: read 1 start\n" +
    			         "Tx 3: read 1 end\n" +
    			         "Tx 2: read 2 end\n" +
    			         "Tx 4: write 1 end\n" +
    			         "Tx 4: read 2 start\n" +
    			         "Tx 4: read 2 end\n";

      if (!result.equals(correctResult))
         System.out.println("*****TxTest: bad tx history");
   }
   
   public synchronized static void appendToResult(String s) {
      result += s + "\n";
   }
}

class Test9A implements Runnable {
   public void run() {
      try {
         Transaction tx = new Transaction();
         Block blk1 = new Block("aFile6", 1);
         Block blk2 = new Block("aFile6", 2);
         tx.pin(blk1);
         tx.pin(blk2);
         Test9.appendToResult("Tx 2: read 1 start");
         tx.getInt(blk1, 0);
         Test9.appendToResult("Tx 2: read 1 end");
         Thread.sleep(2000);
         Test9.appendToResult("Tx 2: read 2 start");
         tx.getInt(blk2, 0);
         Test9.appendToResult("Tx 2: read 2 end");
         tx.commit();
      }
      catch(InterruptedException e) {};
   }
}

class Test9B implements Runnable {
   public void run() {
      try {
         Thread.sleep(1000);
         Transaction tx = new Transaction();
         Block blk1 = new Block("aFile6", 1);
         Block blk2 = new Block("aFile6", 2);
         tx.pin(blk1);
         tx.pin(blk2);
         Test9.appendToResult("Tx 3: write 2 start");
         tx.setInt(blk2, 0, 100);
         Test9.appendToResult("Tx 3: write 2 end");
         Thread.sleep(1500);
         Test9.appendToResult("Tx 3: read 1 start");
         tx.getInt(blk1, 0);
         Test9.appendToResult("Tx 3: read 1 end");
         tx.commit();
      }
      catch(InterruptedException e) {};
   }
}

class Test9C implements Runnable {
   public void run() {
      try {
         Thread.sleep(1500);
         Transaction tx = new Transaction();
         Block blk1 = new Block("aFile6", 1);
         Block blk2 = new Block("aFile6", 2);
         tx.pin(blk1);         
         tx.pin(blk2);
         Test9.appendToResult("Tx 4: write 1 start");
         tx.setInt(blk1, 0, 100);
         Test9.appendToResult("Tx 4: write 1 end");
         Test9.appendToResult("Tx 4: read 2 start");
         tx.getInt(blk2, 0);
         Test9.appendToResult("Tx 4: read 2 end");

//         tx.commit();
      }
      catch(InterruptedException e) {};
   }
}
 
/*
<SETSTRING 1 [file fldcat.tbl, block 1] 228 >
<SETINT 1 [file fldcat.tbl, block 1] 276 0>
<SETINT 1 [file fldcat.tbl, block 1] 252 0>
<SETINT 1 [file fldcat.tbl, block 1] 248 0>
<COMMIT 1>
<START 2>
<START 3>
<SETINT 3 [file aFile6, block 2] 0 0>
<NQCHECKPOINT  2 3>                ===>  TX 2 ve 3 aktif iken NQCKPT baþladý.
<START 4>
<COMMIT 3>
<COMMIT 2>							==> Aktif TXlar (2 ve3) sonlandi. END CKPT yazmali..
<SETINT 4 [file aFile6, block 1] 0 1>
<END NQCKPT>
<START 5>
<COMMIT 5>
*/
