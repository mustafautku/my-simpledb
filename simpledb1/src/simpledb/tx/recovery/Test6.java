package simpledb.tx.recovery;

import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/*
 * Bu test case'de 3 harketin eþ zamanlý çalýþmasý organize edilmiþtir. Bu testi çalýþtýrýken herhangi bir TX PASÝF/AKTÝF CKPT
 * ATMAMASI gerekiyor. Bunun için Transaction.java da constructorda çaðýrýlan  dealWithCheckPoint(txnum); fonksiyonunu comment etmeniz yeterli...
 * Karþýlýk gelen seri plan T3,T2,T4 dur.
 * Bu test sonuda olmasý gereken log dosyasý aþaðýda listelenmiþtir. 
 */

public class Test6{
	private static String result = "";

	public static void main(String[] args){
		System.out.println("BEGIN RECOVERY PACKAGE TEST: ## 6 ");
		SimpleDB.init("testrecovery");

		// TX 2:  R(1), R(2)
		Test6A t2 = new Test6A();
		Thread th2 = new Thread(t2);
		th2.start();

		// TX 3:  W(2), R(1)
		Test6B t3 = new Test6B();
		Thread th3 = new Thread(t3);
		th3.start();

		//TX 4:   W(1), R(2)
		Test6C t4 = new Test6C();
		Thread th4 = new Thread(t4);
		th4.start();

		// concurrent execution
		try{
			th2.join();
			th3.join();
			th4.join();
		}catch (InterruptedException e){
		}

		// TX 5 is only for debug.
		Transaction tx = new Transaction();
		tx.commit();
		tx.listLog();

		//Çalýþma planý aþaðýdaki gibi olmali. Çünkü harektler içinde beklemeler ile (Thread.sleep(1500);  gibi)  bu plana zorlandý. 
		// Plan T3,T2, T1 ve hepsi de rollback yapýyor.
		String correctResult = "Tx 2: read 1 start\n" + "Tx 2: read 1 end\n" + "Tx 3: write 2 start\n" + "Tx 3: write 2 end\n" + "Tx 4: write 1 start\n" + "Tx 2: read 2 start\n" + "Tx 3: read 1 start\n" + "Tx 3: read 1 end\n" + "Tx 2: read 2 end\n" + "Tx 4: write 1 end\n" + "Tx 4: read 2 start\n" + "Tx 4: read 2 end\n";
		if(!result.equals(correctResult))
			System.out.println("*****TxTest: bad tx history");
	}

	public synchronized static void appendToResult(String s){
		result += s + "\n";
	}
}

class Test6A implements Runnable{
	public void run(){
		try{
			Transaction tx = new Transaction();
			Block blk1 = new Block("aFile6", 1);
			Block blk2 = new Block("aFile6", 2);
			tx.pin(blk1);
			tx.pin(blk2);
			Test6.appendToResult("Tx 2: read 1 start");
			tx.getInt(blk1, 0);
			Test6.appendToResult("Tx 2: read 1 end");
			Thread.sleep(2000);
			Test6.appendToResult("Tx 2: read 2 start");
			tx.getInt(blk2, 0);
			Test6.appendToResult("Tx 2: read 2 end");
			tx.rollback();
		}catch (InterruptedException e){
		}
		;
	}
}

class Test6B implements Runnable{
	public void run(){
		try{
			Thread.sleep(1000);
			Transaction tx = new Transaction();
			Block blk1 = new Block("aFile6", 1);
			Block blk2 = new Block("aFile6", 2);
			tx.pin(blk1);
			tx.pin(blk2);
			Test6.appendToResult("Tx 3: write 2 start");
			tx.setInt(blk2, 0, 0);
			Test6.appendToResult("Tx 3: write 2 end");
			Thread.sleep(1500);
			Test6.appendToResult("Tx 3: read 1 start");
			tx.getInt(blk1, 0);
			Test6.appendToResult("Tx 3: read 1 end");
			tx.rollback();
		}catch (InterruptedException e){
		}
		;
	}
}

class Test6C implements Runnable{
	public void run(){
		try{
			Thread.sleep(1500);
			Transaction tx = new Transaction();
			Block blk1 = new Block("aFile6", 1);
			Block blk2 = new Block("aFile6", 2);
			tx.pin(blk1);
			tx.pin(blk2);
			Test6.appendToResult("Tx 4: write 1 start");
			tx.setInt(blk1, 0, 0);
			Test6.appendToResult("Tx 4: write 1 end");
			Test6.appendToResult("Tx 4: read 2 start");
			tx.getInt(blk2, 0);
			Test6.appendToResult("Tx 4: read 2 end");
			tx.rollback();
		}catch (InterruptedException e){
		}
		;
	}
}

/*
......
<SETINT 1 [file fldcat.tbl, block 1] 276 0>
<SETINT 1 [file fldcat.tbl, block 1] 252 0>
<SETINT 1 [file fldcat.tbl, block 1] 248 0>
<COMMIT 1>
<START 2>
<START 3>
<SETINT 3 [file aFile6, block 2] 0 0>
<START 4>
<ROLLBACK 3>
<ROLLBACK 2>
<SETINT 4 [file aFile6, block 1] 0 1>
<ROLLBACK 4>
<START 5>
<COMMIT 5>

*/
