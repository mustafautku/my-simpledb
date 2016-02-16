package testextsort;


import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import simpledb.file.FileMgr;
import simpledb.materialize.TempTable;
import simpledb.metadata.MetadataMgr;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.RecordFile;
import simpledb.record.RecordPage;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.record.TempRecordPage;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class Split1Merge2Test1 {
	static Schema sch;
	static List<String> sf;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ServerInit1Table.initData("testExtSort");
		MetadataMgr md = SimpleDB.mdMgr();
		Transaction tx = new Transaction();
		TableInfo ti = md.getTableInfo("student", tx);
		sch = ti.schema();
		sf = Arrays.asList("sid");
		
		//test #0:
		System.out.println("**********test #0: list all students.***************");
		RecordFile rf = new RecordFile(ti, tx);
		while (rf.next())
			System.out.println(rf.currentRid() + " " + rf.getInt("sid"));
		rf.close();
		System.out.println("**********end of test 0***************");
		
		//test #1:
		System.out.println("**********test #1: list all students in first page.Then find the smallest in the page.***************");
		TempRecordPage trp = new TempRecordPage(ti, 0, tx);
		while (trp.next())
			System.out.println(trp.currentId() + " " + trp.getVal("sid"));
		System.out.println(trp.findSmallestFrom(0, sf));
		trp.close();
		System.out.println("**********end of test 1***************");

		//test #2:ts'nin iþaret ettiði deðere en yakýn büyük-eþit deðeri ilk TempRecordPage'de bulmak
		System.out.println("**********test #2: ts'nin iþaret ettiði deðerden büyük eþit bir deðeri page içerisinde bulmak***************");
		TableScan ts=new TableScan(ti,tx);
		ts.beforeFirst();
		ts.moveToRid(new RID(40,4));	// ts points here	
		trp = new TempRecordPage(ti, 0, tx);
		System.out.println(trp.findSmallestAsBigAs(ts, sf));
		trp.close();
		ts.close();
		System.out.println("**********end of test 2***************");
		
		
		//test #3: internal sort only a page
		System.out.println("**********test #3: page içerindeki kayýtlarý sýralama ***************");
		trp = new TempRecordPage(ti, 0, tx);
		trp.internalSort(sf);
		trp.printPage();
		trp.close();
		System.out.println("**********end of test 3***************");
			
		
		tx.commit();
	}
	
		
}
