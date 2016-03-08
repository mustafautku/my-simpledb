package testskyline;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import simpledb.materialize.TempTable;
import simpledb.metadata.MetadataMgr;
import simpledb.multibuffer.ChunkScan;
import simpledb.query.IntConstant;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.query.TableScan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/*
 * Generate double data.
 * 
 * 
 * */

public class Test4 {
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SimpleDB.BUFFER_SIZE=500;
	
//		InitIntData.initData("skyline");
		
		InitDoubleData.initData("skylineDouble"); // skyline isimli ornek bir VT.
											// Icerisinde INPUT(A double, B double, C varchar(30))tablosu var. 
		Transaction tx = new Transaction();
		MetadataMgr md = SimpleDB.mdMgr();
//		TableInfo ti = md.getTableInfo("input", tx);
		
		Plan p = new TablePlan("input", tx);
		
		// Test 1: List all data:
//		Scan input = p.open();
//		while(input.next()){
//			double A=input.getDouble("A");
//			double B=input.getDouble("B");
//			System.out.println(A + ",  " + B);
//		}
//		input.close();
		
		// Test 2: List data with A and B out of area [-2,+2]
//		UpdateScan updateinput=(UpdateScan)p.open();
//		while(updateinput.next()){
//			double A=updateinput.getDouble("a");
//			double B=updateinput.getDouble("b");
//			if((A<-2 || A>2))// && (B<-2 || B>2) ) 
//				System.out.println(A + ",  " + B);
//		}
		
	
	}
}
