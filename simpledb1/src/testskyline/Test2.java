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
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/*
 *SKYLINE " 1 ITERATION " 
 * 
 * 
 * */

public class Test2 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		InitIntData.initData("skyline"); // skyline isimli ornek bir VT.
											// Icerisinde INPUT(A int, B int, C varchar(30))
											// tablosu var. Tabloda 999 tane
											// kayýt var.
		Transaction tx = new Transaction();
		MetadataMgr md = SimpleDB.mdMgr();
		TableInfo ti = md.getTableInfo("input", tx);
		Schema sch = ti.schema();

		int counterInWindow = 0;
		int counterInTemp = 0;
		/*
		 * input tablosunundaki kayýtlarý sýrayla okuyoruz. Burda 1 tampon(page)
		 * kullanýyor.
		 */

		Plan p = new TablePlan("input", tx);
		Scan input = p.open();
//		input.next();
		/*
		 * Ana hafizadaki WINDOW bölgesinin set edilmesi: 5 tampon yer kapliyor.
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

		/*
		 * Bu ornekte input dosyasýndan WINDOW sýðacak kadar kayýt okuyoruz.
		 * Sonra Window dolduktan sonra diðer kayýtlarý tempfile'a yazacagiz.
		 */

//		while (window.insertFromScan(input)) { // copy data from input to chunk
//												// area
//			counterInWindow++;
//			if (!input.next()) {
//				break;
//			}
//		}
//		
//		System.out.println("Window'da olanlar: " + counterInWindow); // TOPLAM
																		// 33* 5
																		// = 165
																		// tane
																		// olmalý.

		doAnIteration(input, window,output);
		
		System.out.println(" (AFTER 1 ITERATION) WINDOW AREA: ");
		window.printChunk();
		System.out.println("  (AFTER 1 ITERATION) TEMP AREA: ");
		output.beforeFirst();
		while(output.next()){
			int oA=output.getInt("A");
			int oB=output.getInt("B");
			System.out.println(oA + ", " + oB);
		}
		
		input.close();
		window.close();
		output.close();

	}
	
	static void doAnIteration(Scan input,ChunkScan window, TableScan output){
		
		while (input.next()) {
			int A = input.getInt("A");
			int B = input.getInt("B");
			window.beforeFirst();
			boolean willBeInwindow = true;
			while (window.next()) {
				int wA = window.getInt("A");
				int wB = window.getInt("B");
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
				window.beforeFirst();
				boolean windowEnough = window.insert();
				if (windowEnough) {
					window.setVal("A", new IntConstant(A));
					window.setVal("B", new IntConstant(B));
				} else {  // window is full. Should write to output file(temp)
					output.insert();
					output.setInt("A", input.getInt("A"));
					output.setInt("B", input.getInt("B"));
				}
			}

		}

	}
}
