package simpledb.tx.recovery;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class Test2 {
	 private static Block blk = new Block("aFile2", 0);
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		 System.out.println("BEGIN RECOVERY PACKAGE TEST: ## 2 ");
	      SimpleDB.init("testrecovery");
	      Transaction tx=new Transaction();
	      tx.pin(blk);
	      tx.setInt(blk, 4, 100);
	      tx.setInt(blk, 4, 200);
	      tx.commit();
	      tx.listLog();
	}

}

/*
 * TESTRECOVERY D�Z�N�N� S�L�P,Bu program� �LK �ALI�TIRDI�IMIZDA, log dosyas� ekranda a�a��daki gibi listelenmeli..
 *
 * ...... tx=1'e ait katalog dosyalar�n�n olu�turulmas� ile ilgili log kay�tlar� bu k�s�mda...
 * <SETINT 1 [file fldcat.tbl, block 1] 224 0>
<SETSTRING 1 [file fldcat.tbl, block 1] 256 >
<SETSTRING 1 [file fldcat.tbl, block 1] 228 >
<SETINT 1 [file fldcat.tbl, block 1] 276 0>
<SETINT 1 [file fldcat.tbl, block 1] 252 0>
<SETINT 1 [file fldcat.tbl, block 1] 248 0>
<COMMIT 1>
<START 2>
<SETINT 2 [file aFile2, block 0] 4 8>       // Buradaki 8 de�eri farkl� olabilir.
<SETINT 2 [file aFile2, block 0] 4 100>
<COMMIT 2>
*******************************************************
*
*
Mevcut "testrecovery" veritaban�n� S�LMEDEN, Bu program� �K�NC� �ALI�TIRDI�IMIZDA, log dosyas� ekranda a�a��daki gibi listelenmeli..
<SETINT 1 [file fldcat.tbl, block 1] 224 0>
<SETSTRING 1 [file fldcat.tbl, block 1] 256 >
<SETSTRING 1 [file fldcat.tbl, block 1] 228 >
<SETINT 1 [file fldcat.tbl, block 1] 276 0>
<SETINT 1 [file fldcat.tbl, block 1] 252 0>
<SETINT 1 [file fldcat.tbl, block 1] 248 0>
<COMMIT 1>
<START 2>
<SETINT 2 [file aFile2, block 0] 4 8>
<SETINT 2 [file aFile2, block 0] 4 100>
<COMMIT 2>
<START 1>
<CHECKPOINT>        // BUNU S�STEM YEN� BA�LARKEN MEVCUT B�R "testrecovery" vt BULDU�U ���N RECOVERY YAPTI VE ERTES�NDE BU CKPT'Y� BURAYA YAZDI. YOKSA BU CKPT'Y� B�Z TET�KLEMED�K...
<COMMIT 1>
<START 2>
<SETINT 2 [file aFile2, block 0] 4 200>
<SETINT 2 [file aFile2, block 0] 4 100>
<COMMIT 2>

*/