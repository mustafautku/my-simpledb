package testqueryexec;

import java.util.ArrayList;
import java.util.Map;
import simpledb.index.Index;
import simpledb.index.btree.BTreeIndex;
import simpledb.index.hash.HashIndex;
import simpledb.metadata.IndexInfo;
import simpledb.query.IntConstant;
import simpledb.query.Plan;
import simpledb.query.TablePlan;
import simpledb.query.TableScan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/*
 * Bu programda IDX'lerin oluþturulmasýný içeriyor. 
 * MEvcut simpledb VT'i (yoksa) yükleme sonra IDX'ler (yoksa) yükleme iþlemleri var..
 * Basit bir sorgu: 3 nolu dep'teki ogrenci id'lerini idx kullanarak ekrana basiyor.
 */

public class QTest2 {
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String dbname = "studentdb";
		LoadStudentDB.initData(dbname);

		Transaction tx = new Transaction();
		LoadStudentDB.setDataStatistics(tx); // set the field stats according to
												// sciori's book.
		LoadStudentDB.getDataStatistics(tx);

		// Load IDXs if NOT exist:
		System.out.println("\nINDEX COORDINATION:");
		System.out.println("----------------------");
		loadIndex("dept", "did", "DID_shashIDX", "shash", tx);
		loadIndex("dept", "did", "DID_btreeIDX", "btree", tx);
		loadIndex("student","sid","SID_btreeIDX","btree", tx);
		loadIndex("student","majorid","MajorID_shashIDX","shash", tx);
//		loadIndex("course","cid","CId_IDX",tx);
		// loadIndex("section","sectid","SectId_IDX",tx);
		// loadIndex("enroll","studentid","StudentId_IDX",tx);

		 Map<String,ArrayList<IndexInfo>>
		 studentindexes=SimpleDB.mdMgr().getIndexInfo("student", tx);
		 ArrayList<IndexInfo> majorididxs=studentindexes.get("majorid");
		 IndexInfo ii=majorididxs.get(0); //get the fisrtindex..
		 Index idx=ii.open();
		 idx.beforeFirst(new IntConstant(3));
		 TableInfo ti=SimpleDB.mdMgr().getTableInfo("student", tx);
		 UpdateScan s=new TableScan(ti,tx);
		
		 while(idx.next()){
			s.moveToRid(idx.getDataRid());
			int sid = s.getInt("sid");
			int majorid = s.getInt("majorid");
			System.out.println(majorid + ": " + sid);
		 }
		
		tx.rollback();
	}

	private static void loadIndex(String TABLEname, String FIELDname,
			String IDXname, String indextype, Transaction tx) {
		Map<String, ArrayList<IndexInfo>> indexes = SimpleDB.mdMgr().getIndexInfo(
				TABLEname, tx);
		ArrayList<IndexInfo> ii = indexes.get(FIELDname);
		if (ii != null){
			for(int i=0;i<ii.size();i++){
				IndexInfo aIndex=ii.get(i);
				if(aIndex.getIndexType().equalsIgnoreCase(indextype)){
					System.out.println(IDXname + " already exist...");
					return;
				}
			}			
		}

		Plan p = new TablePlan(TABLEname, tx);
		UpdateScan s = (UpdateScan) p.open(); // UpdateScan is only for getRID

		Schema idxsch = new Schema();
		idxsch.addIntField("dataval");
		idxsch.addIntField("block");
		idxsch.addIntField("id");
		Index idx = null;
		if (indextype.equalsIgnoreCase("btree"))
			idx = new BTreeIndex(IDXname, idxsch, tx);
		else if (indextype.equalsIgnoreCase("shash"))
			idx = new HashIndex(IDXname, idxsch, tx);

		System.out.println("loading " + IDXname + "...");
		while (s.next()) {
			idx.insert(s.getVal(FIELDname), s.getRid());
		}
		s.close();
		idx.close();
		SimpleDB.mdMgr().createIndex(IDXname, TABLEname, FIELDname, indextype,
				tx);

	}
}
