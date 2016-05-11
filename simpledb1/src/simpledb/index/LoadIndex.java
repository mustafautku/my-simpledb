package simpledb.index;

import java.util.ArrayList;
import java.util.Map;

import simpledb.index.Index;
import simpledb.index.btree.BTreeIndex;
import simpledb.index.hash.HashIndex;
import simpledb.metadata.IndexInfo;
import simpledb.query.Plan;
import simpledb.query.TablePlan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class LoadIndex {
	public static Index loadIndex(String TABLEname, String FIELDname,
			String IDXname, String indextype, Transaction tx) {
		Map<String, ArrayList<IndexInfo>> indexes = SimpleDB.mdMgr()
				.getIndexInfo(TABLEname, tx);
		ArrayList<IndexInfo> ii = indexes.get(FIELDname);
		boolean alreadyExist=false;
		if (ii != null) {
			for (int i = 0; i < ii.size(); i++) {
				IndexInfo aIndex = ii.get(i);
				if (aIndex.getIndexType().equalsIgnoreCase(indextype)) {
					System.out.println(IDXname + " already exist...");
					alreadyExist=true;
//					return alreadyExist;
					return null;
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
//		idx.close();
//		if (indextype.equalsIgnoreCase("btree"))
//			System.out.println(((BTreeIndex) idx).getStatistics().toString());
		SimpleDB.mdMgr().createIndex(IDXname, TABLEname, FIELDname, indextype,
				tx);
//		return alreadyExist;
		return idx;
	}
}