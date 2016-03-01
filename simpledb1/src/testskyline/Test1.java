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
 * Bu testin amac� 8 adet tampon b�lgesinin koordinasyonunu anlamaktir. Tamponumuz 3 k�sma ayr�ldi.
 * 1 tampon: input dosyas�ndan gelen kay�tlar
 * 5 tampon: window b�lgemiz.
 * 2 tampon: tempfile'a yazana output buffer'dir. Tempfile'in geni�lemesi i�in auni anda 2 tampona ihtiya� var.
 */

public class Test1 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		InitOneTable.initData("skyline"); // skyline isimli ornek bir VT.
											// Icerisinde INPUT(A int, B int)
											// tablosu var. Tabloda 999 tane
											// kay�t var.
		Transaction tx = new Transaction();
		MetadataMgr md = SimpleDB.mdMgr();
		TableInfo ti = md.getTableInfo("input", tx);
		Schema sch = ti.schema();
		
		int counterInWindow = 0;
		int counterInTemp = 0;
		/*
		 * input tablosunundaki kay�tlar� s�rayla okuyoruz. Burda 1 tampon(page)
		 * kullan�yor.
		 */
			
		Plan p=new TablePlan("input",tx);
		Scan input = p.open();
		input.next();
		/*
		 * Ana hafizadaki WINDOW b�lgesinin set edilmesi: 5 tampon yer kapliyor.
		 * Icerisinde formatlanm�� bo� slotlar var. Artik bu bolgeyi
		 * kullanabiliriz.
		 */
		ChunkScan window= new ChunkScan(5, sch, tx);
		/*
		 * TEMP dosyam�z: Window'a s��mayanlar� buraya yazacagiz..Geni�lerken
		 * ayn� anda 2 tampon yer kapl�yor..
		 */
		TempTable outputfile = new TempTable(ti.schema(), tx);
		TableScan output = (TableScan) outputfile.open();

		// BufferMgr bm=SimpleDB.bufferMgr(); // debug amacli isterseniz tampon
		// havuzuna bakin do�ru taksimat olmus mu?

		/*
		 * Bu ornekte input dosyas�ndan WINDOW s��acak kadar kay�t okuyoruz.
		 * Sonra Window dolduktan sonra di�er kay�tlar� tempfile'a yazacagiz.
		 */
	
		while (window.insertFromScan(input)){ // copy data from input to chunk area
			counterInWindow++;
			if (!input.next()) {
				break;
			}
		}
		System.out.println("Window'da olanlar: " + counterInWindow); // TOPLAM 33* 5 = 165 tane olmal�.

		
		output.insert(); // en son okudu�umuzu yazamamistik. Once onu
							// yaz�yoruz..
		output.setInt("A", input.getInt("A"));
		output.setInt("B", input.getInt("B"));
		counterInTemp++;
		
		while (input.next()) { // output icin bir s�n�r yok. istedi�i kadar
								// b�y�yebiir..
			output.insert(); // tempfile genislerken 2 tane tampon tutuyor. O
								// yuzden Windoe size'i 5 yapmak zorunda
								// kaldik..			
			output.setInt("A", input.getInt("A"));
			output.setInt("B", input.getInt("B"));
			counterInTemp++;
		}
		
		System.out.println("Toplam: " + (counterInWindow + counterInTemp));
		/*
		 * Bu k�s�mda window b�lesindeki baz� kay�tlar� i�aretleme ile ilgili bir ornek. ��aretleme yerine baz� kay�tlar�n
		 * (bu ornekte A de�eri <100 olanlar�) RID de�erlerini saklayalim. Sonra buralara tekrar gidip bu kay�tlr� temp file'a 
		 * kopyalayal�m, bu arada window'dan da silelim..
		 */
		window.beforeFirst();
		ArrayList<RID> list=new ArrayList<RID>();
		while(window.next()){
			if(window.getInt("A")<100)
				list.add(window.getRid());			
		}
		System.out.println("Window'daki <100 olan elemanlar: toplam "+ list.size() + " eleman tempfile'a aktar�l�yor.");
		Iterator<RID> iter=list.iterator();
		while(iter.hasNext()){
			window.moveToRid(iter.next());
			output.insert();  //tempfile'da yer bul
			output.setInt("A", window.getInt("A"));
			output.setInt("B", window.getInt("B"));
			window.delete();		// baska bir slota gecmedik. Bulundugumuz slotu empty yapacak.	
		}
		
		
		//Simdi window'da daha az eleman kalm�� olmas� gerek. Bakalim:
		counterInWindow=0;
		window.beforeFirst();
		while(window.next()){
			if(window.getInt("A")<100)
				System.err.print("silememisiz maalesef.");
			counterInWindow++;		
		}
		System.out.println("Window'da geride kalanlar: " + counterInWindow); // Bu ornek icin <165 tane olmal�. 
		
		input.close();
		window.close();
		output.close();
		
	}

}
