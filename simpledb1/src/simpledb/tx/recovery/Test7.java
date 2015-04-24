package simpledb.tx.recovery;

import simpledb.tx.*;
import simpledb.file.*;
import simpledb.server.SimpleDB;

/*
 * Bu test case'de önceki gibi 3 hareketin eþ zamanlý çalýþmasý organize edilmiþtir. 
 * Bu test TX4 un PASÝF CKPT atmasýný test ediyor. 
 * Bunun için Transaction.javada constructorda çaðýrýlan  dealWithCheckPoint(txnum); fonksiyonunu comment etmeniz yeterli...
 * N (CKPT threshold)=4 olarak set edin.
 * Bu test sonuda olmasý gereken log dosyasý aþaðýda listelenmiþtir. 
 */


public class Test7 {
   private static String result = "";
   
   public static void main(String[] args) {
	   System.out.println("BEGIN RECOVERY PACKAGE TEST: ## 7 ");
      SimpleDB.init("testrecovery");

      // TX 2:  R(1), R(2)
      Test7A t2 = new Test7A();
      Thread th2 = new Thread(t2);
      th2.start();
      
      // TX 3:  W(2), R(1)
      Test7B t3 = new Test7B();
      Thread th3 = new Thread(t3);
      th3.start();
      
      //TX 4:   W(1), R(2)
      Test7C t4 = new Test7C();
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
         "Tx 2: read 2 start\n" +   // tx2 DEVAM EDEMÝYOR. çÜNKÜ XLOCK(2) TUTULUYOR.
         "Tx 3: read 1 start\n" +
         "Tx 3: read 1 end\n" +
         "Tx 2: read 2 end\n" +
         "Tx 4: write 1 start\n" +
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

class Test7A implements Runnable {
   public void run() {
      try {
         Transaction tx = new Transaction();
         Block blk1 = new Block("aFile6", 1);
         Block blk2 = new Block("aFile6", 2);
         tx.pin(blk1);
         tx.pin(blk2);
         Test7.appendToResult("Tx 2: read 1 start");
         tx.getInt(blk1, 0);
         Test7.appendToResult("Tx 2: read 1 end");
         Thread.sleep(2000);
         Test7.appendToResult("Tx 2: read 2 start");
         tx.getInt(blk2, 0);
         Test7.appendToResult("Tx 2: read 2 end");
         tx.commit();
      }
      catch(InterruptedException e) {};
   }
}

class Test7B implements Runnable {
   public void run() {
      try {
         Thread.sleep(1000);
         Transaction tx = new Transaction();
         Block blk1 = new Block("aFile6", 1);
         Block blk2 = new Block("aFile6", 2);
         tx.pin(blk1);
         tx.pin(blk2);
         Test7.appendToResult("Tx 3: write 2 start");
         tx.setInt(blk2, 0, 100);
         Test7.appendToResult("Tx 3: write 2 end");
         Thread.sleep(1500);
         Test7.appendToResult("Tx 3: read 1 start");
         tx.getInt(blk1, 0);
         Test7.appendToResult("Tx 3: read 1 end");
         tx.commit();
      }
      catch(InterruptedException e) {};
   }
}

class Test7C implements Runnable {
   public void run() {
      try {
         Thread.sleep(1500);
         Transaction tx = new Transaction();
         Block blk1 = new Block("aFile6", 1);
         Block blk2 = new Block("aFile6", 2);
         tx.pin(blk1);         
         tx.pin(blk2);
         Test7.appendToResult("Tx 4: write 1 start");
         tx.setInt(blk1, 0, 100);
         Test7.appendToResult("Tx 4: write 1 end");
         Test7.appendToResult("Tx 4: read 2 start");
         tx.getInt(blk2, 0);
         Test7.appendToResult("Tx 4: read 2 end");

//         tx.commit();
      }
      catch(InterruptedException e) {};
   }
}
 
/*
<SETINT 1 [file fldcat.tbl, block 1] 276 0>
<SETINT 1 [file fldcat.tbl, block 1] 252 0>
<SETINT 1 [file fldcat.tbl, block 1] 248 0>
<COMMIT 1>
<START 2>
<START 3>
<SETINT 3 [file aFile6, block 2] 0 0>
<COMMIT 3>
<COMMIT 2>
<CHECKPOINT>      ====> BURASI ONEMLÝ: AKTÝF TX LAR( TX3 VE TX2) SONLANDIKTAN SONRA CKPT LOG KAYDINI YAZDI. SONRA TX4 BAÞLAYABÝLDÝ..
<START 4>
<SETINT 4 [file aFile6, block 1] 0 1>      ===> Dikkat: TX4 sonlanmadi. Bir sonraki Test8'de geri sarmasý gerekiyor...
<START 5>
<COMMIT 5>

*/
