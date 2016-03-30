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
 * Show the Generated double data. 
 * 
 * */

public class Test5 {
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SimpleDB.BUFFER_SIZE=500;
		ArrayList<Double> AllPointsI=new ArrayList<Double>();
		ArrayList<Double> AllPointsC=new ArrayList<Double>();
		ArrayList<Double> AllPointsU=new ArrayList<Double>();
			
		InitData.initData("skyline100"); 
		
		Transaction tx = new Transaction();		
		
		System.out.println("independent data (uniform dist)");
		System.out.println("-------------------------------");
		Plan p = new TablePlan("input", tx);
		Scan input = p.open();
		input.beforeFirst();
		// Test 1: List all DOUBLE data:		
		while (input.next()) {
			double A = input.getDouble("aind");
			double B = input.getDouble("bind");
			AllPointsI.add(A);
			AllPointsI.add(B);
			System.out.println(A + "  " + B);
			
			A = input.getDouble("acor");
			B = input.getDouble("bcor");
			AllPointsC.add(A);
			AllPointsC.add(B);
			System.out.println(A + "  " + B);
			
			A = input.getDouble("aunc");
			B = input.getDouble("bunc");
			AllPointsU.add(A);
			AllPointsU.add(B);
			System.out.println(A + "  " + B);
		}
		input.close();
		
//		System.out.println("\ndependent(correlated) data (gaussian dist)");
//		System.out.println("----------------------------------------------");
//		p = new TablePlan("inputC", tx);
//		input = p.open();
//		input.beforeFirst();
//		// Test 1: List all DOUBLE data:		
//		while (input.next()) {
//			double A = input.getDouble("a");
//			double B = input.getDouble("b");
//			AllPointsC.add(A);
//			AllPointsC.add(B);
//			System.out.println(A + "  " + B);
//		}
//		input.close();
//		
//		System.out.println("\ndependent(UNcorrelated) data (gaussian dist)");
//		System.out.println("----------------------------------------------");
//		p = new TablePlan("inputU", tx);
//		input = p.open();
//		input.beforeFirst();
//		// Test 1: List all DOUBLE data:		
//		while (input.next()) {
//			double A = input.getDouble("a");
//			double B = input.getDouble("b");
//			AllPointsU.add(A);
//			AllPointsU.add(B);
//			System.out.println(A + "  " + B);
//		}	
//		input.close();
		
		DataGraphic gt1 = new DataGraphic(AllPointsI,new ArrayList<Double>());  // skyline set size=0
		DataGraphic gt2 = new DataGraphic(AllPointsC,new ArrayList<Double>());  // skyline set size=0
		DataGraphic gt3 = new DataGraphic(AllPointsU,new ArrayList<Double>());  // skyline set size=0
	
	}
}
