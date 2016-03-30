package testskyline;

import java.util.ArrayList;
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
 *  SKYLINE on (aint  INT ,bint   INT) ONLY 1 ITERATION. 
 *  
 * */

public class Test2a {

	/**
	 * @param args
	 */
	static Schema sch;
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		InitData.initData("skyline100"); 
		
		Transaction tx = new Transaction();
		MetadataMgr md = SimpleDB.mdMgr();
		TableInfo ti = md.getTableInfo("input", tx);
		Schema sch = ti.schema();

		/*
		 * input tablosunundaki kayýtlarý sýrayla okuyoruz. Burda 1 tampon(page)
		 * kullanýyor.
		 */

		Plan p = new TablePlan("input", tx);
		Scan input = p.open();
		input.beforeFirst();
//		input.next();
		/*
		 * Ana hafizadaki WINDOW bölgesinin set edilmesi: k=1,2,3... tampon yer kapliyor.
		 * Icerisinde formatlanmýþ boþ slotlar var. Artik bu bolgeyi
		 * kullanabiliriz.
		 */
		ChunkScan window = new ChunkScan(1, sch, tx);
		/*
		 * TEMP dosyamýz: Window'a sýðmayanlarý buraya yazacagiz..Geniþlerken
		 * ayný anda 2 tampon yer kaplýyor..
		 */
		TempTable outputfile = new TempTable(ti.schema(), tx);
		TableScan output = (TableScan) outputfile.open();

		// BufferMgr bm=SimpleDB.bufferMgr(); // debug amacli isterseniz tampon
		// havuzuna bakin doðru taksimat olmus mu?

		doAnIteration(input, window,output);
		
		System.out.println(" (AFTER 1 ITERATION) WINDOW AREA: ");
		window.printChunk();
		System.out.println("  (AFTER 1 ITERATION) TEMP AREA: ");
		output.beforeFirst();
		while(output.next()){
			int oA=output.getInt("aint");
			int oB=output.getInt("bint");
			System.out.println(oA + ", " + oB);
		}
		input.close();
		window.close();
		output.close();

	}
	
	static void doAnIteration(Scan input,ChunkScan window, TableScan output){
		
		while (input.next()) {
			int A = input.getInt("aint");
			int B = input.getInt("bint");
			window.beforeFirst();
			boolean willBeInwindow = true;
			while (window.next()) {
				int wA = window.getInt("aint");
				int wB = window.getInt("bint");
				if ((A <= wA && B < wB) || (A < wA && B <= wB)) { // better at
																	// least one
																	// dim. ==>
																	// dominate
																	// in
																	// the
																	// window
					window.delete();
					// willBeInwindow=true;
				} else if ((wA <= A && wB < B) || (wA < A && wB <= B)) { // better
																			// at
																			// least
																			// one
																			// dim.
																			// ==>
																			// dominate
																			// the
																			// input.
					willBeInwindow = false;
					break;
				}
			}
			if (willBeInwindow) {
//				window.beforeFirst();
//				boolean windowEnough = window.insertAvailable() ; burda insert yapýp veri yazmazsa o slot dolu hale geliyor, manasiz bilgi ile harcamiþ olyoruz.  O yuzden çýkardým.
				if (!window.insertFromScan(input)) {
					// window is full. Should write to output file(temp)
//					output.insert();
//					output.setInt("aint", input.getInt("aint"));
//					output.setInt("bint", input.getInt("bint"));
//					window.copyToScan(output);
					transferBwScans(input,output);
				}
			}

		}

	}
	static boolean transferBwScans(Scan s1, UpdateScan s2) {
		s2.insert();
		for (String fldname : sch.fields())
			s2.setVal(fldname, s1.getVal(fldname));
		return true;
	}
}
