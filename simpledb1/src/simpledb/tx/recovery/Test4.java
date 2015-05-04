package simpledb.tx.recovery;

import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/*
 * Bu test case Test3'e benziyor. Fakat TX rollback yap�yor. Bu program�n ��kt�s� a�a��daki gibi olmal�..Bu test case'den sonra
 * �al��t�raca��n�z Test 5 ��kt�s� ise herhangi bir hata vermeden sonlanmal�... 
 */

public class Test4 {

	private static Block blk = new Block("aFile4", 0);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("BEGIN RECOVERY PACKAGE TEST: ## 4 ");
		SimpleDB.init("testrecovery");
		// tx:2 starting
		Transaction tx = new Transaction();
		tx.pin(blk);
		tx.setInt(blk, 0, 100);
		tx.commit();
//		tx.listLog();
		// tx:3 starting. (set N (CKPT threshold) as 3 so that this tx throws a
		// quiesent(pasif) CKPT !!)
		tx = new Transaction();
		tx.pin(blk);
		tx.setInt(blk, 4, 200);
		
		// Bu a�a��da bu �ekilde sistem ayakta iken recover yapmak do�ru de�il. recover sadece sistem yeni ba�larken yapmak gerek.
//		Transaction tx2 = new Transaction(); // this tx is used for recovery..
//		tx2.recover();
//		tx2.pin(blk);
//		if (tx2.getInt(blk, 0) != 100 || tx2.getInt(blk, 4) == 200)
//			System.out.println("*****RecoveryTest: FAIL!!!");
//		tx2.commit();
		
//		Bunun yerine program� sonland�ral�m. a�a��y� comment'ledim. Yani tx3 commit olmadan programsonlans�n. Test5.java'y�  �al��t�rd���m�zda
//		recovery yapacak, Ve 0.byte'da 100 olmal�. 4.byte'da ise 200 OLMAMALI...
//		tx.commit();
		tx.listLog();	 
	}
}

/*
<SETINT 1 [file fldcat.tbl, block 1] 276 0>
<SETINT 1 [file fldcat.tbl, block 1] 252 0>
<SETINT 1 [file fldcat.tbl, block 1] 248 0>
<COMMIT 1>
<START 2>
<SETINT 2 [file aFile4, block 0] 0 1>
<COMMIT 2>
<CHECKPOINT>        
<START 3>
<SETINT 3 [file aFile4, block 0] 4 8>
*/