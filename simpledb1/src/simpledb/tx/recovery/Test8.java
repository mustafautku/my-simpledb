package simpledb.tx.recovery;

import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/*
 * Bu test case  ANCAK Test7.java dan SONRA �al��t�r�lablilir. Mevcut "testrecovery" veritaban�n� S�LMEY�N. Sistem ilk ba�larken recover yapacak o kadar. 
 * Sonra TX2 do�ru recover yapt���n� kontrol ediyor. Bu Program�n FAIL!!!  yazmadan hatasiz sonlanmas� gerekiyor..
 * Bununla beraber log dosyas�n�n son hali a�a��daki gibi olmal�d�r...
 * 
 */

public class Test8 {

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
		// blk1'i de�i�tiren hareket(TX4) geri sardi o zaman artik 0'da 100 olmamal�.
		// blk2'i de�i�tiren hareket(TX3) geri sarmadi, cunku CKPT oncesinde o zaman yapt�g� de�i�iklik ge�erli..
		if (tx.getInt(blk1, 0) == 100 || tx.getInt(blk2, 0) != 100)   
			System.out.println("*****RecoveryTest: FAIL!!!");
		tx.commit();
		tx.listLog();	 
		
	}
}

/*
 * .........
<SETINT 1 [file fldcat.tbl, block 1] 276 0>
<SETINT 1 [file fldcat.tbl, block 1] 252 0>
<SETINT 1 [file fldcat.tbl, block 1] 248 0>
<COMMIT 1>
<START 2>
<START 3>
<SETINT 3 [file aFile6, block 2] 0 0>
<COMMIT 3>
<COMMIT 2>
<CHECKPOINT>
<START 4>
<SETINT 4 [file aFile6, block 1] 0 1>
<START 5>
<COMMIT 5>    				==>  Test7 buraya kadar yazm��t�...
<START 1>
<CHECKPOINT>				==>> System recovery burda oldu..
<COMMIT 1>
<START 2>
<COMMIT 2>
*/
