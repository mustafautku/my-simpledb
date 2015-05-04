package simpledb.tx.recovery;

import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/*
 * Bu test case'de aktif hareket yokken ortaya ��kan bir hareketin PAS�F CKPT yapmas�d�r.
 * PRogram �al��t���nda sondaki gibi bir ��kt� ekranda g�z�kmeli. N=s�n�r de�eri = 3 olarak belirlersen 3. hareketin
 * ba�lamas� ancak CKPT bittikten sonra olabilir. Bu testte 3. hareket ba�larken e�zamanl� ba�ka bir hareket olmad��� i�in 
 * hemen CKPT yapmas� ---ve ilgli log kayd�n� dosyaya yazmas�-- ve sonra ba�lamas� gerekiyor.
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
  
............ tx=1'e ait katalog dosyalar�n�n olu�turulmas� ile ilgili log kay�tlar� bu k�s�mda...
<SETSTRING 1 [file fldcat.tbl, block 1] 256 >
<SETSTRING 1 [file fldcat.tbl, block 1] 228 >
<SETINT 1 [file fldcat.tbl, block 1] 276 0>
<SETINT 1 [file fldcat.tbl, block 1] 252 0>
<SETINT 1 [file fldcat.tbl, block 1] 248 0>
<COMMIT 1>
<START 2>
<SETINT 2 [file aFile3, block 0] 0 1>
<COMMIT 2>
<CHECKPOINT>             //  BURADA CKPT ATMASI GEREK�YOR. BUNU S�STEM DE��L, TX3 TET�KLED�.. !!!!!HERHANG� AKT�F BA�KA B�R TX OLMADI�INDAN HEMEN CKPT YAZDI VE TX3 BA�LADI..
<START 3>
<SETINT 3 [file aFile3, block 0] 4 8>
<COMMIT 3>
*/