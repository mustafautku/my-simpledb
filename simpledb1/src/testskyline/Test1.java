package testskyline;

import java.util.ArrayList;
import java.util.Iterator;
import simpledb.materialize.TempTable;
import simpledb.metadata.MetadataMgr;
import simpledb.multibuffer.ChunkScan;
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
 * Bu testin amacý 8 adet tampon bölgesinin koordinasyonunu anlamaktir. Tamponumuz 3 kýsma ayrýldi.
 * 1 tampon: input dosyasýndan gelen kayýtlar
 * 5 tampon: window bölgemiz.
 * 2 tampon: tempfile'a yazana output buffer'dir. Tempfile'in geniþlemesi için auni anda 2 tampona ihtiyaç var.
 */

public class Test1 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		InitData.initData("skyline100"); // record size=96,  slot size=100,  Block SIZE=400, RPB=4,
		
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
			
		Plan p=new TablePlan("input",tx);
		Scan input = p.open();
		input.next();
		/*
		 * Ana hafizadaki WINDOW bölgesinin set edilmesi: 5 tampon yer kapliyor.
		 * Icerisinde formatlanmýþ boþ slotlar var. Artik bu bolgeyi
		 * kullanabiliriz.
		 */
		ChunkScan window= new ChunkScan(5, sch, tx);
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
	
		while (window.insertFromScan(input)){ // copy data from input to chunk area
			counterInWindow++;
			if (!input.next()) {
				break;
			}
		}
		System.out.println("Window'da olanlar: " + counterInWindow); // TOPLAM: RPB(=4)* 5 = 20 tane olmalý.

		
		output.insert(); // en son okuduðumuzu yazamamistik. Once onu
							// yazýyoruz..
		output.setInt("aint", input.getInt("aint"));
		output.setInt("bint", input.getInt("bint"));
		counterInTemp++;
		
		while (input.next()) { // output icin bir sýnýr yok. istediði kadar
								// büyüyebiir..
			output.insert(); // tempfile genislerken 2 tane tampon tutuyor. O
								// yuzden Windoe size'i 5 yapmak zorunda
								// kaldik..			
			output.setInt("aint", input.getInt("aint"));
			output.setInt("bint", input.getInt("bint"));
			counterInTemp++;
		}
		
		System.out.println("Toplam: " + (counterInWindow + counterInTemp));
		/*
		 * Bu kýsýmda window bölesindeki bazý kayýtlarý iþaretleme ile ilgili bir ornek. Ýþaretleme yerine bazý kayýtlarýn
		 * (bu ornekte A deðeri <10 olanlarý) RID deðerlerini saklayalim. Sonra buralara tekrar gidip bu kayýtlrý temp file'a 
		 * kopyalayalým, bu arada window'dan da silelim..
		 */
		int threshold=10;
		window.beforeFirst();
		ArrayList<RID> list=new ArrayList<RID>();
		while(window.next()){
			if(window.getInt("aint")<threshold)
				list.add(window.getRid());			
		}
		System.out.println("Window'daki <"+threshold+ " olan elemanlar: toplam "+ list.size() + " eleman tempfile'a aktarýlýyor.");
		Iterator<RID> iter=list.iterator();
		while(iter.hasNext()){
			window.moveToRid(iter.next());
			output.insert();  //tempfile'da yer bul
			output.setInt("aint", window.getInt("aint"));
			output.setInt("bint", window.getInt("bint"));
			window.delete();		// baska bir slota gecmedik. Bulundugumuz slotu empty yapacak.	
		}
		
		
		//Simdi window'da daha az eleman kalmýþ olmasý gerek. Bakalim:
		counterInWindow=0;
		window.beforeFirst();
		while(window.next()){
			if(window.getInt("aint")<threshold)
				System.err.print("silememisiz maalesef.");
			counterInWindow++;		
		}
		System.out.println("Window'da geride kalanlar: " + counterInWindow); 
		
		System.out.println("Temp'da olanlar: ");
		counterInTemp=0;
		output.beforeFirst();
		while(output.next()){
			int oA=output.getInt("aint");
			int oB=output.getInt("bint");
//			System.out.println(oA + ", " + oB);
			counterInTemp++;
		}
		System.out.println("Toplam: " + (counterInWindow + counterInTemp));
		input.close();
		window.close();
		output.close();
		
	}

}
