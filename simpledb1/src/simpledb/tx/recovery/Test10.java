package simpledb.tx.recovery;

import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/*
 * Bu test case  ANCAK Test9.java dan SONRA çalýþtýrýlablilir. Mevcut "testrecovery" veritabanýný SÝLMEYÝN. Sistem ilk baþlarken recover yapacak o kadar. 
 * Sonra TX2 doðru recover yaptýðýný kontrol ediyor. Bu Programýn FAIL!!!  yazmadan hatasiz sonlanmasý gerekiyor..
 * Bununla beraber log dosyasýnýn son hali aþaðýdaki gibi olmalýdýr...
 * 
 */

public class Test10 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("BEGIN RECOVERY PACKAGE TEST: ## 8 ");
		SimpleDB.init("testrecovery");
		// tx:2 starting
		Transaction tx = new Transaction();  // sadece okuma yapan kontrol amacli bir hareket..
		Block blk1 = new Block("aFile6", 1);
        Block blk2 = new Block("aFile6", 2);
		tx.pin(blk1);
		tx.pin(blk2);
		// blk1'i deðiþtiren hareket(TX4) geri sardi o zaman artik 0'da 100 olmamalý.
		// blk2'i deðiþtiren hareket(TX3) geri sarmadi, cunku CKPT oncesinde o zaman yaptýgý deðiþiklik geçerli..
		if (tx.getInt(blk1, 0) == 100 || tx.getInt(blk2, 0) != 100)   
			System.out.println("*****RecoveryTest: FAIL!!!");
		tx.commit();
		tx.listLog();	 
		
	}
}

/*
 * .........
<SETINT 1 [file fldcat.tbl, block 1] 252 0>
<SETINT 1 [file fldcat.tbl, block 1] 248 0>
<COMMIT 1>
<START 2>
<START 3>
<SETINT 3 [file aFile6, block 2] 0 0>
<NQCHECKPOINT  2 3>
<START 4>
<COMMIT 3>
<COMMIT 2>
<SETINT 4 [file aFile6, block 1] 0 1>
<END NQCKPT>
<START 5>
<COMMIT 5>
<START 1>
<CHECKPOINT>			==> system recovery burda oluyor...
<COMMIT 1>
<START 2>
<COMMIT 2>
*/
