package simpledb.buffer;

import simpledb.file.FileMgr;
import simpledb.metadata.MetadataMgr;
import simpledb.query.ProductScan;
import simpledb.query.Scan;
import simpledb.query.TableScan;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/*  LRU ve MRU karsýlasþtýrma yapýlacak. MRU ile caliþtýrýnca daha iyi bir sonuç alýnýyor.
 * LRU'da sequential flooding denilen problem oluyor.
 * Buffer size artýnca (sag tablonun boyutuna yakýnlaþtýkça) MRU daha da iyileþtiði görülebilir.
 *  
 *  Page size: 800
 *  80 kayýt,8 block : student
 *  7 kayýt, 1 block: dept
*/

public class Test2 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LoadStudentDeptDB.initData("studentdb");
		FileMgr fm=SimpleDB.fileMgr();
		MetadataMgr mdMgr = SimpleDB.mdMgr();
		Transaction tx = new Transaction();
		
		int ReadCounter=fm.blocksRead();// 17 : 9 from data, 8 from initialize (log&catalog@recovery)
		int WriteCounter=fm.blocksWritten();		// 3 (log last block write for recovery and log append)
		System.out.println("Total Blocks READ:" +ReadCounter +
				   "\nTotal Blocks WRITTEN:" + WriteCounter);	
		
		TableInfo dti = mdMgr.getTableInfo("dept", tx);  
		TableInfo sti = mdMgr.getTableInfo("student", tx);
		ReadCounter=fm.blocksRead();// 0,1,2 falan : (log&catalog)
	
		
		
		boolean refreshmentOK = SimpleDB.bufferMgr().refreshment();  // for only debug purposes.
		if(!refreshmentOK)
			System.exit(0);
		Scan s1=new TableScan (dti,tx);
		Scan s2=new TableScan (sti,tx);
//		ReadCounter=fm.blocksRead();// 0,1,2 falan : (log&catalog)
		
		Scan s3=new ProductScan (s1,s2);
//		ReadCounter=fm.blocksRead();// 0,1,2 falan : (log&catalog)
	
		while(s3.next());
		//LRU prints  =  READS: 1+ 7*8 = 57
		//MRU prints  =  READS: 1+ 8 + ( [8-(buffersize-1)]*(7-1))  buffersize=8   ==> 15   bundan sonra SON s2.next() student 0. bloðunu tekrar tampona alýyor. O yüzden 16.
 		System.out.println("Total Blocks READ:" +(fm.blocksRead()-ReadCounter) +
				   "\nTotal Blocks WRITTEN:" + (fm.blocksWritten()-WriteCounter) );	   		
		s3.close();
		
	}

}
