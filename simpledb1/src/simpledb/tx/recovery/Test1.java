package simpledb.tx.recovery;

import simpledb.server.SimpleDB;
import simpledb.file.Block;
import simpledb.buffer.*;

/*
 * original recovery test.Bu testte sadece recovery sýnýfýnda rollback ve recovery nasýl yapýlýyor onu goruyoruz. Bu fonksiyonlar
 * LogIterator ile log dosyasýnda geriye doðru tarama yaparak hareket(ler)i kurtarýyrlar ve sonra rollback veya recovery log kayýtlarýný
 * (yani recovery için CKPT log kaydýný)log dosyasýna yazýyorlar. Program çýktýsý FAIL!!! vermemeli. Log dosyasý da programýn sonunda 
 * listelendigi sekilde olmalý..
 */
public class Test1 {
   private static Block blk = new Block("aFile1", 12);
   private static BufferMgr bm;
   
  public static void main(String[] args)  {
      System.out.println("BEGIN RECOVERY PACKAGE TEST");
      SimpleDB.init("testrecovery");
      bm = SimpleDB.bufferMgr();
      Buffer buff = bm.pin(blk);
      buff.setInt(4, 9876, 1, -1);
      buff.setString(20, "abcdefg", 1, -1);
      buff.setString(40, "hijk", 1, -1);
      buff.setInt(104, 9999, 1, -1);
      buff.setString(120, "gfedcba", 1, -1);
      buff.setString(140, "kjih", 1, -1);
      bm.flushAll(1);
      bm.unpin(buff);
      
      testRollback();//SADECE bu çalýþtýðý zaman programýn sonunda listenen log çýktýsý olmasý gerekiyor.
//      testRecover();    //SADECE bu çalýþtýðý zaman programýn sonunda listenen log çýktýsý olmasý gerekiyor..
   }
   
   private static void testRollback() {
	   
      // first, log and make changes to the block's values
      int txid = 234;
      RecoveryMgr rm = new RecoveryMgr(txid);
//      rm.listLog();
      Buffer buff = bm.pin(blk);
      int lsn = rm.setInt(buff, 4, 1234);
      buff.setInt(4, 1234, txid, lsn);
      lsn = rm.setString(buff, 20, "xyz");
      buff.setString(20, "xyz", txid, lsn);
      bm.unpin(buff);
      bm.flushAll(txid);
      
      // verify that the changes got made
      buff = bm.pin(blk);
      if (buff.getInt(4) != 1234 || !buff.getString(20).equals("xyz"))
         System.out.println("*****RecoveryTest: FAIL!!!! rollback changes not made");
      bm.unpin(buff);
      
      rm.rollback();
      // verify that they got rolled back
      buff = bm.pin(blk);
      if (buff.getInt(4) != 9876
             || !buff.getString(20).equals("abcdefg"))
         System.out.println("*****RecoveryTest: FAIL!!!! bad rollback");
      bm.unpin(buff);
      
      rm.listLog();
   }
   
   private static void testRecover() {
      // use different txs to log and make changes to those values
      int txid1 = 456;
      int txid2 = 457;
      int txid3 = 458;
      RecoveryMgr rm1 = new RecoveryMgr(txid1);
      RecoveryMgr rm2 = new RecoveryMgr(txid2);
      RecoveryMgr rm3 = new RecoveryMgr(txid3);
      
      Buffer buff = bm.pin(blk);
      int lsn = rm1.setInt(buff, 104, 1234);
      buff.setInt(104, 1234, txid1, lsn);
      lsn = rm2.setString(buff, 120, "xyz");
      buff.setString(120, "xyz", txid2, lsn);
      lsn = rm3.setString(buff, 140, "rst");
      buff.setString(140, "rst", txid3, lsn);
      bm.unpin(buff);
      
      // verify that the changes got made
      buff = bm.pin(blk);
      if (buff.getInt(104) != 1234 || !buff.getString(120).equals("xyz")
             || !buff.getString(140).equals("rst"))
         System.out.println("*****RecoveryTest: FAIL!!!recovery changes not made");
      bm.unpin(buff);
      rm2.commit();
      rm1.recover();
      // verify that txs 456 and 458 got rolled back
      buff = bm.pin(blk);
      if (buff.getInt(104) != 9999
             || !buff.getString(120).equals("xyz")
             || !buff.getString(140).equals("kjih"))
         System.out.println("*****RecoveryTest: FAIL!!!bad recovery");
      bm.unpin(buff);
      
      RecoveryMgr rmtest = new RecoveryMgr(100);  // for only test purposes.
      rmtest.commit();
      rmtest.listLog();
      
   }
}

/* SADECE TESTROLLBACK() CAGIRILDIGI ZAMAN ASAGÝDAKÝ GÝBÝ LÝSTELENMELÝ... 
.....tx=1'e ait katalog dosyalarýnýn oluþturulmasý ile ilgili log kayýtlarý bu kýsýmda...
<SETSTRING 1 [file fldcat.tbl, block 1] 228 >
<SETINT 1 [file fldcat.tbl, block 1] 276 0>
<SETINT 1 [file fldcat.tbl, block 1] 252 0>
<SETINT 1 [file fldcat.tbl, block 1] 248 0>
<COMMIT 1>
<START 234>
<SETINT 234 [file aFile1, block 12] 4 9876>
<SETSTRING 234 [file aFile1, block 12] 20 abcdefg>
<ROLLBACK 234>
*
*
*
*

/* TESTRECOVERY DÝZÝNÝNÝ SÝLÝP, SADECE TESTRECOVER() CAGIRILDIGI ZAMAN ASAGÝDAKÝ GÝBÝ LÝSTELENMELÝ... 
.....tx=1'e ait katalog dosyalarýnýn oluþturulmasý ile ilgili log kayýtlarý bu kýsýmda...
<SETSTRING 1 [file fldcat.tbl, block 1] 228 >
<SETINT 1 [file fldcat.tbl, block 1] 276 0>
<SETINT 1 [file fldcat.tbl, block 1] 252 0>
<SETINT 1 [file fldcat.tbl, block 1] 248 0>
<COMMIT 1>
<START 456>
<START 457>
<START 458>
<SETINT 456 [file aFile1, block 12] 104 9999>
<SETSTRING 457 [file aFile1, block 12] 120 gfedcba>
<SETSTRING 458 [file aFile1, block 12] 140 kjih>
<COMMIT 457>
<CHECKPOINT>      ==> TX-456  recovery'i cagirdi..
<START 100>
<COMMIT 100>
*/