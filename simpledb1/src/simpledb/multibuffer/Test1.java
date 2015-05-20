package simpledb.multibuffer;

import java.util.ArrayList;
import java.util.Iterator;
import simpledb.materialize.TempTable;
import simpledb.metadata.MetadataMgr;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.RecordFile;
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

		InitOneTable.initData("skyline"); // skyline isimli ornek bir VT.
											// Icerisinde INPUT(A int, B int)
											// tablosu var. Tabloda 999 tane
											// kayýt var.
		Transaction tx = new Transaction();
		MetadataMgr md = SimpleDB.mdMgr();

		/*
		 * input tablosunundaki kayýtlarý sýrayla okuyoruz. Burda 1 tampon(page)
		 * kullanýyor.
		 */
		int size = tx.size("input.tbl");
		TableInfo ti = md.getTableInfo("input", tx);
		int recLen = ti.recordLength();
		RecordFile input = new RecordFile(ti, tx);

		/*
		 * Ana hafizadaki WINDOW bölgesinin set edilmesi: 5 tampon yer kapliyor.
		 * Icerisinde formatlanmýþ boþ slotlar var. Artik bu bolgeyi
		 * kullanabiliriz.
		 */
		TableInfo tiWindow = new TableInfo("tempWindowFile", ti.schema()); // dosya
																			// ismi
																			// "temp"
																			// ile
																			// baslasin
																			// ta
																			// ki
																			// system
																			// yeni
																			// basladiginda
																			// bu
																			// dosyayý
																			// silsin..Bkz.
																			// FileMgr
																			// constructor
		WindowUpdateScan window = new WindowUpdateScan(tiWindow, 0, 4, tx);

		/*
		 * TEMP dosyamýz: Window'a sýðmayanlarý buraya yazacagiz..Geniþlerken
		 * ayný anda 2 tampon yer kaplýyor..
		 */
		TempTable temp = new TempTable(ti.schema(), tx);
		TableScan tempfile = (TableScan) temp.open();

		// BufferMgr bm=SimpleDB.bufferMgr(); // debug amacli isterseniz tampon
		// havuzuna bakin doðru taksimat olmus mu?

		/*
		 * Bu ornekte input dosyasýndan WINDOW sýðacak kadar kayýt okuyoruz.
		 * Sonra Window dolduktan sonra diðer kayýtlarý tempfile'a yazýyoruz.
		 */
		int A = 0, B = 0;
		System.out.println("Window'da olanlar"); // TOPLAM 33* 5 = 198 tane
													// olmalý.
		int counterInWindow = 0;
		int counterInTemp = 0;

		while (input.next()) {
			if (window.insert()) {
				A = input.getInt("A");
				B = input.getInt("B");
				window.setInt("A", A);
				window.setInt("B", B);
				System.out.println(A + " " + B);
				counterInWindow++;
			} else
				break;
		}
		System.out.println("Toplam window kayýtlarýnýn sayýsý: "
				+ (counterInWindow));

		System.out.println("Temp dosyasýna gidenler");
		tempfile.insert(); // en son okuduðumuzu yazamamistik. Once onu
							// yazýyoruz..
		tempfile.setInt("A", A);
		tempfile.setInt("B", B);
		System.out.println(A + " " + B);
		counterInTemp++;
		while (input.next()) { // tempfile icin bir sýnýr yok. istediði kadar
								// büyüyebiir..
			tempfile.insert(); // tempfile genislerken 2 tane tampon tutuyor. O
								// yuzden Windoe size'i 5 yapmak zorunda
								// kaldik..
			A = input.getInt("A");
			B = input.getInt("B");
			tempfile.setInt("A", A);
			tempfile.setInt("B", B);
			System.out.println(A + " " + B);
			counterInTemp++;
		}
		
		System.out.println("Toplam: " + (counterInWindow + counterInTemp));
		/*
		 * Bu kýsýmda window bölesindeki bazý kayýtlarý iþaretleme ile ilgili bir ornek. Ýþaretleme yerine bazý kayýtlarýn
		 * (bu ornekte A deðeri <100 olanlarý) RID deðerlerini saklayalim. Sonra buralara tekrar gidip bu kayýtlrý temp file'a 
		 * kopyalayalým, bu arada window'dan da silelim..
		 */
		window.beforeFirst();
		ArrayList<RID> list=new ArrayList<RID>();
		while(window.next()){
			if(window.getInt("A")<100)
				list.add(window.getRid());			
		}
		System.out.println("toplam "+ list.size() + " eleman tempfile'a aktarýlýyor");
		Iterator<RID> iter=list.iterator();
		while(iter.hasNext()){
			window.moveToRid(iter.next());
			A=window.getInt("A");
			B=window.getInt("B");
			tempfile.insert();  //tempfile'da yer bul
			tempfile.setInt("A", A);
			tempfile.setInt("B", B);
			window.delete();			
		}
		
		input.close();
		window.close();
		tempfile.close();
		
	}

}
