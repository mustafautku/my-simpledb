package testskyline;

import static simpledb.file.Page.BLOCK_SIZE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import simpledb.materialize.TempTable;
import simpledb.metadata.MetadataMgr;
import simpledb.multibuffer.ChunkScan;
import simpledb.multibuffer.SkylineBNLPlan;
import simpledb.query.DoubleConstant;

import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/*
 * 
 * 
 * 
 * */

public class Test6 {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SimpleDB.BUFFER_SIZE=80;
//		int WINDOWSIZE=5; // en fazla "buffer size-3" olabilir.
		
		
	
		List<String> fields = Arrays.asList("aind", "bind");
		
		InitData.initData("skyline100"); // skyline isimli ornek bir VT.
											// Icerisinde INPUT(int id,aint int,bint int,......, C varchar(8))  // record length=96,  slotsize= 100
											// tablosu var. 		
		
		
		Transaction tx = new Transaction();
		ArrayList<Double> AllPoints=InitData.getAllPoints(fields,tx);
		ArrayList<Double> SKYLINEPoints=new ArrayList<Double>();
		
				
		Plan source = new TablePlan("input", tx);
		
		
		SkylineBNLPlan bnlplan = new SkylineBNLPlan(source, fields, tx);
		Scan bnlscan=bnlplan.open();
		while(bnlscan.next()){
			int id=bnlscan.getInt("id");
			double rsx=bnlscan.getDouble(fields.get(0));
			double rsy=bnlscan.getDouble(fields.get(1));
			SKYLINEPoints.add(rsx);
			SKYLINEPoints.add(rsy);
			System.out.println(id+ ": "+rsx + " "+rsy);
		}
		DataGraphic gt1 = new DataGraphic(AllPoints,SKYLINEPoints);
		
		System.out.println("There are " + SKYLINEPoints.size()/2 + " skyline points");
	}
	
	
}
