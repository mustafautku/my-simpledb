package testextsort;


import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import simpledb.file.FileMgr;
import simpledb.materialize.TempTable;
import simpledb.metadata.MetadataMgr;
import simpledb.multibuffer.ChunkScan;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.RecordFile;
import simpledb.record.RecordPage;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.record.TempRecordPage;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class TempChunkScanTest1 {
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
		ChunkScan chunk;
		//test #1:
//		System.out.println("**********test #1: list all students in first 2 pages.Then find the smallest in the CHUNK.***************");
//		chunk = new ChunkScan(ti, 0,1, tx);
////		while (chunk.next())
////			System.out.println(chunk.getVal("sid"));
//		chunk.printChunk();
//		int[] smallest=chunk.findSmallestFrom(0,0, sf);
//		System.out.println(smallest[0] + " "+smallest[1]);
//		chunk.close();
//		System.out.println("**********end of test 1***************");
//
//		//test #2:
		System.out
				.println("*** *******test #2: internal sort of CHUNK.***************");
//		TempTable temp = new TempTable(sch, tx);
//		TableInfo tiTemp = temp.getTableInfo();
//		trp = new TempRecordPage(tiTemp, 0, tx);
		chunk = new ChunkScan(8, sch, tx);
		Plan p=new TablePlan("student",tx);
		Scan src = p.open();
		src.next();
		while (chunk.insertFromScan(src)) // copy data from src to chunk area
			if (!src.next()) {
				break;
			}
		System.out.println("unsorted chunk is below:");
		chunk.printChunk();		
		System.out.println("-----------------------");
		
		chunk.internalSort(sf);
		
		System.out.println("sorted chunk is below:");		
		chunk.printChunk();
		chunk.close();
		System.out.println("**********end of test 2***************");			
//		tx.rollback();  // rollback gerek yok.
		tx.commit();
	}
	
		
}
