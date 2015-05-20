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

		/*
		 * input tablosunundaki kay�tlar� s�rayla okuyoruz. Burda 1 tampon(page)
		 * kullan�yor.
		 */
		int size = tx.size("input.tbl");
		TableInfo ti = md.getTableInfo("input", tx);
		int recLen = ti.recordLength();
		RecordFile input = new RecordFile(ti, tx);

		/*
		 * Ana hafizadaki WINDOW b�lgesinin set edilmesi: 5 tampon yer kapliyor.
		 * Icerisinde formatlanm�� bo� slotlar var. Artik bu bolgeyi
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
																			// dosyay�
																			// silsin..Bkz.
																			// FileMgr
																			// constructor
		WindowUpdateScan window = new WindowUpdateScan(tiWindow, 0, 4, tx);

		/*
		 * TEMP dosyam�z: Window'a s��mayanlar� buraya yazacagiz..Geni�lerken
		 * ayn� anda 2 tampon yer kapl�yor..
		 */
		TempTable temp = new TempTable(ti.schema(), tx);
		TableScan tempfile = (TableScan) temp.open();

		// BufferMgr bm=SimpleDB.bufferMgr(); // debug amacli isterseniz tampon
		// havuzuna bakin do�ru taksimat olmus mu?

		/*
		 * Bu ornekte input dosyas�ndan WINDOW s��acak kadar kay�t okuyoruz.
		 * Sonra Window dolduktan sonra di�er kay�tlar� tempfile'a yaz�yoruz.
		 */
		int A = 0, B = 0;
		System.out.println("Window'da olanlar"); // TOPLAM 33* 5 = 198 tane
													// olmal�.
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
		System.out.println("Toplam window kay�tlar�n�n say�s�: "
				+ (counterInWindow));

		System.out.println("Temp dosyas�na gidenler");
		tempfile.insert(); // en son okudu�umuzu yazamamistik. Once onu
							// yaz�yoruz..
		tempfile.setInt("A", A);
		tempfile.setInt("B", B);
		System.out.println(A + " " + B);
		counterInTemp++;
		while (input.next()) { // tempfile icin bir s�n�r yok. istedi�i kadar
								// b�y�yebiir..
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
		System.out.println("toplam "+ list.size() + " eleman tempfile'a aktar�l�yor");
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
