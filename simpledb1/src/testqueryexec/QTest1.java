package testqueryexec;

import simpledb.metadata.MetadataMgr;
import simpledb.metadata.StatInfo;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/*Bu programda Sciori DB kitab�ndaki STUDENT VT istatistiksel bilgilerine uygun "studentdb" isimli bir veri taban� olu�turulmu�tur. 
 * V(.) de�erleri i�in Stat bilgileri runtime'da olu�turuluyor. ��nk� md bu de�erler i�in yakla��k (1/3) de�er gonderiyor.
* "studentdb" isimli VT onceden mevcutsa takrar olu�tu-mu-yor. Ancak nitelik istatistiklerini set ediyor.
* 
STUDENT: B=4500   R=45000    Vsid=45.000       Vsname=45.000     Vgradyear=50  Vmajorid=40
DEPT:    B=2      R=40       Vdid=40           Vdname=40
COURSE:  B=25     R=500      Vcid=500          Vtitle=500        Vdeptid=40
SECTION: B=2500   R=25000    Vsectid=25000     Vcourseid=500     Vprof=250     Vyearoffered=50
ENROLL:  B=50000  R=1500000  Vsectionid=25000  Vstudentid=45000  Vgrade=14
*/

public class QTest1 {
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		LoadStudentDB.initData("studentdb");
		Transaction tx = new Transaction();
		LoadStudentDB.getDataStatistics(tx);  // #ofBlocks and #ofRECORDS and V() => 1/3 selectivity 
		
		LoadStudentDB.setDataStatistics(tx);  // set ALL statistics into md.
		LoadStudentDB.getDataStatistics(tx);  //
		
		tx.commit();
	}
	
	

}

