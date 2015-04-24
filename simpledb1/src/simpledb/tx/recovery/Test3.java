package simpledb.tx.recovery;

import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/*
 * Bu test case'de aktif hareket yokken ortaya çýkan bir hareketin PASÝF CKPT yapmasýdýr.
 * PRogram çalýþtýðýnda sondaki gibi bir çýktý ekranda gözükmeli. N=sýnýr deðeri = 3 olarak belirlersen 3. hareketin
 * baþlamasý ancak CKPT bittikten sonra olabilir. Bu testte 3. hareket baþlarken eþzamanlý baþka bir hareket olmadýðý için 
 * hemen CKPT yapmasý ---ve ilgli log kaydýný dosyaya yazmasý-- ve sonra baþlamasý gerekiyor.
 */

public class Test3 {

	 private static Block blk = new Block("aFile3", 0);
		/**
		 * @param args
		 */
		public static void main(String[] args) {
			// TODO Auto-generated method stub
			 System.out.println("BEGIN RECOVERY PACKAGE TEST: ## 3 ");
		      SimpleDB.init("testrecovery");
		      // tx:2 starting
		      Transaction tx=new Transaction();
		      tx.pin(blk);
		      tx.setInt(blk, 0, 100);
		      tx.commit();
		      tx.listLog();
		      // tx:3 starting. (set N (CKPT threshold) as 3 so that this tx throws a quiesent(pasif) CKPT !!)
		      tx=new Transaction();
		      tx.pin(blk);
		      tx.setInt(blk, 4, 200);
		      tx.commit();
		      tx.listLog();	      
		      
		}
}
 /*
  
............ tx=1'e ait katalog dosyalarýnýn oluþturulmasý ile ilgili log kayýtlarý bu kýsýmda...
<SETSTRING 1 [file fldcat.tbl, block 1] 256 >
<SETSTRING 1 [file fldcat.tbl, block 1] 228 >
<SETINT 1 [file fldcat.tbl, block 1] 276 0>
<SETINT 1 [file fldcat.tbl, block 1] 252 0>
<SETINT 1 [file fldcat.tbl, block 1] 248 0>
<COMMIT 1>
<START 2>
<SETINT 2 [file aFile3, block 0] 0 1>
<COMMIT 2>
<CHECKPOINT>             //  BURADA CKPT ATMASI GEREKÝYOR. BUNU SÝSTEM DEÐÝL, TX3 TETÝKLEDÝ.. !!!!!HERHANGÝ AKTÝF BAÞKA BÝR TX OLMADIÐINDAN HEMEN CKPT YAZDI VE TX3 BAÞLADI..
<START 3>
<SETINT 3 [file aFile3, block 0] 4 8>
<COMMIT 3>
*/