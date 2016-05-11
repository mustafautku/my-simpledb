package testindexing;

import simpledb.index.*;
import simpledb.index.query.*;
import simpledb.index.btree.BTreeIndex;
import simpledb.metadata.IndexInfo;
import simpledb.query.*;
import simpledb.record.*;
import simpledb.server.SimpleDB;

import simpledb.tx.Transaction;

import java.util.*;

/*
 * RUN TESTS IN GROUP INDEPENDENTLY. For example, << run testInsert1();  testQuery1(); >>  together.
 * YOU CAN RUN ALL OF THEM MANY TIMES..
 * 
 * ***** >>  BLOCK SIZE = 100.   DIR block holds at most 10. (when becomes 11,split)  LEAF blocks holds AT MOST 6.(when becomes 7,split)
 * 
 * Test 1: sid idx:      V(sid)=200:  200/3= 66 leafs. log(10)66 =2 en iyi aðaç. 
 * 						Fakat her DIR düðümü 5 idx kaydý tutyor. O zaman 66 /5 = 13,   13/5=2,  1 root
 * 						Tree structure=      1 2 13 | 66 , 0 OFs
 * 						Block ids:
 * 						root:            0
 * 						level 1:   13          12
 * 						level 0:  2 3 4 5 6    7 8 9 10 11 12 13 14
 *                      leafs:  0 1 2 3 4       . . . . . . .. . .  .66
 * Test 2: majorid idx:  V(majorid)=4       
 * 						200 /4= 50 for each majorid. Thus 4 leafs. Each OF block holds 6 records. 50/6 = 8. Thus 6*8 = 48 OF
 * 						Tree structure:   1 |4, 32 OFs (4*8)
 * 
 * Test 3: COURSE.cid idx:  secondary idx.
 * 
 * Speacial Test 4: gradyear idx: V(gradyear)=1 come from DB. But we add 4 more diff values. Thus V becomes 5. 
 * 						Tree structure:  1 | 3, 35 OFs.
 * 						Block id'leri:
 *    					root:          0
 *    					Leafs:   0     34        35
 *    					OFs:   36  (1,2,3,....33)   37
 *    					0.leaf içeriði: 2000,2000, 2002,2003
 *    					34. leaf içerigi: 2016,2016
 *  				    35 leaf içeriði: 2020,2020
 * 
 * NOT: Sometimes all tests execution ( developing new indexes) may not run.Maybe resource problem. But, seperate executions always run normally.
 * 
 */
public class BTreeTest {
	private static Schema idxsch = new Schema();
	private static Transaction tx;

	public static void main(String[] args) {
		System.out.println("BEGIN BTREE PACKAGE TEST");
		ServerInit.initData("testindexing");
		tx = new Transaction();
		// idxsch.addIntField("majorid"); // bu olmamasý gerek. kitapta yanliþ..
		idxsch.addIntField("dataval");
		idxsch.addIntField("block");
		idxsch.addIntField("id");

		BTreeIndex idx=null;
		Constant searchkey=null;
		Constant[] searchRange=null;
		
		//test #1:
		idx=loadIndex("student", "sid", "SID_btree","btree", tx);
		idx.resetQueryStats();
		searchkey = new IntConstant(11);
		testPointQuery("student", "sid",searchkey,idx);  // student.SID_IDX unique and primary index(on ordered data). No  duplicates.
		idx.resetQueryStats();
		searchRange = new Constant[]{new IntConstant(45),new IntConstant(60)};
		testRangeQuery("student", "sid",searchRange,idx);
		
		//test #2:
		idx=loadIndex("student", "majorid", "MAJORID_btree","btree", tx);
		idx.resetQueryStats();
		searchkey = new IntConstant(2);
		testPointQuery("student", "majorid",searchkey,idx);  // student.MAJORID_IDX: some values replicate.
		idx.resetQueryStats();
		searchRange = new Constant[]{new IntConstant(2),new IntConstant(3)};
		testRangeQuery("student", "majorid",searchRange,idx);
		
		//test #3:
		idx=loadIndex("course", "cid", "CID_btree","btree", tx);
		idx.resetQueryStats();
		searchkey = new IntConstant(20);
		testPointQuery("course", "cid", searchkey, idx);  // student.MAJORID_IDX: some values replicate.
		idx.resetQueryStats();
		searchRange = new Constant[]{new IntConstant(20),new IntConstant(101)};
		testRangeQuery("course", "cid",searchRange,idx);
		
		// special test #4: Mostly repeated values with few special additions.
		specialTestPointQueries();


		
//		testDelete();
//		testIndexSelect();
// 		testIndexJoin();

		tx.commit();
	}

	private static BTreeIndex loadIndex(String tablename,String colname,String idxname, String type,Transaction tx){
		System.out.println("			TEST # : "+ idxname + " characteristics ::::::::");
		System.out.println("		-------------------------------------------------------");
		BTreeIndex idx = (BTreeIndex) LoadIndex.loadIndex(tablename, colname, idxname,
				type, tx);
		if(idx==null){ // already exist.
			Map<String, ArrayList<IndexInfo>> tableindexes = SimpleDB.mdMgr()
					.getIndexInfo(tablename, tx);
			ArrayList<IndexInfo> idxs = tableindexes.get(colname);
			IndexInfo ii = idxs.get(0); // get the fisrtindex..
			idx = (BTreeIndex) ii.open();
			System.out.println(idx.calculateStatistics());
		}
		else{  // new idx.
			System.out.println(idx.getStatistics().toString());  // Aðaç yapýsý : yukseklik=3: 1 2 13 | 66 leafs, 0 OF
		}
		System.out.println();
		return idx;
	}
	
	
	private static void testPointQuery(String tablename, String attr, Constant searchkey, BTreeIndex idx) {

		System.out.println("POINT QUERY on ( "+ tablename + ", " + attr + ")::::::::");
		System.out.println("Point Query (Search Key): " + searchkey);
		
		Plan p = new TablePlan(tablename, tx);
		UpdateScan s = (UpdateScan) p.open();
		s.beforeFirst();
		List<RID> rids = new ArrayList<RID>();
		while (s.next())
			if (s.getVal(attr).equals(searchkey))
				rids.add(s.getRid());
		s.close();

		int ridcount = 0;
		idx.beforeFirst(searchkey);
		System.out.print("[ ");
		while (idx.next()) {
			ridcount++;
			RID rid = idx.getDataRid();
			s.moveToRid(rid);
			System.out.print(s.getInt(attr)+" ");
			if (!rids.contains(rid))
				System.out.println("*****BTreeTest: rid not inserted");
		}
		System.out.println(" ]");
		idx.close();
		if (ridcount != rids.size())
			System.err.println("*****BTreeTest: not enough rids inserted");
		else{
			System.out.println("--------> Test #1: POINT QUERY PASSES ...");
			System.out.println("--------> Number of Query results: "+ idx.getStatistics().getQueryResults());
			System.out.println("--------> Number of Disk Access: "+ idx.getStatistics().getReads()+"\n");
		}		
	}
	
	private static void testRangeQuery(String tablename, String attr, Constant[] searchRange, BTreeIndex idx) {  

		System.out.println("RANGE QUERY on ( "+ tablename + ", " + attr + ")::::::::");		
		System.out.println("Range Query (Range Keys): [" + searchRange[0]+ ", "+searchRange[1] + " ]");
		
		Plan p = new TablePlan(tablename, tx);
		UpdateScan s = (UpdateScan) p.open();
		s.beforeFirst();
		List<RID> rids = new ArrayList<RID>();
		while (s.next())
			if (s.getVal(attr).compareTo(searchRange[0])==0 || s.getVal(attr).compareTo(searchRange[1])==0 ||
					(s.getVal(attr).compareTo(searchRange[0])>0 && s.getVal(attr).compareTo(searchRange[1])<0 )  )
				rids.add(s.getRid());
		
		s.beforeFirst();
		
		int ridcount = 0;
		idx.beforeFirst(searchRange);
		System.out.print("[ ");
		while (idx.next()) {
			RID rid =idx.getDataRid();
			if (!rids.contains(rid))
				System.out.println("*****BTreeTest: rid not inserted.");
			s.moveToRid(rid);
			System.out.print(s.getInt(attr) + " ");
			ridcount++;
		}
		System.out.println(" ]");
		idx.close();
		s.close();
		if (ridcount != rids.size())
			System.err.println("*****BTreeTest: not enough rids inserted");
		else {
			System.out.println("Total numbers: " + ridcount);
			System.out.println("--------> Test #1: RANGE QUERY PASSES ...");
			System.out.println("--------> Number of Query results: "+ idx.getStatistics().getQueryResults());
			System.out.println("--------> Number of Disk Access: "+ idx.getStatistics().getReads()+"\n");
		}		
	}

	private static void specialTestPointQueries() { // "almost" all same value inserts
		System.out.println(				"SPECIAL TEST #: GRADYEARID_IDX characteristics  ::::::::");
		System.out.println("		------------------------------------------------------------------");
		BTreeIndex idx=(BTreeIndex)LoadIndex.loadIndex("student", "gradyear", "GRADYEAR_btree",
				"btree", tx);
		if(idx==null){ // already exist.
			Map<String, ArrayList<IndexInfo>> studentindexes = SimpleDB.mdMgr()
					.getIndexInfo("student", tx);
			ArrayList<IndexInfo> gradyearidxs = studentindexes.get("gradyear");
			IndexInfo ii = gradyearidxs.get(0); // get the fisrtindex..
			idx = (BTreeIndex)ii.open();
			System.out.println( idx.calculateStatistics()); // Aðaç yapýsý :yukseklik=1, 218 kayýt. 1 root, 3 leaf, 35 (1+33+1) OF blocks
		}
		else{  // new idx: Sadece yeniidxiseaþaðýdakikayýtlarýekle. Yoksa her seferinde ekleme yapacak..
			System.out.println(idx.getStatistics().toString());  // Aðaç yapýsý : yukseklik=1, 200 kayýt. 1 root, 1 leaf, 33 OF blocks
			
			// Su anda 200 tane 2016 kaydý var. Yeni
			//  2000< 2016 olan bir gradyear kayitlarý ekleyip. Split olmasý
			// gerektigini gorelim. Ve 2020 olan kayýtlar ekleyip leaf'ler saga doðru da ilerlesin.
			// Son olarak 2000'lerin yanýna 2002 ve 2003 girelim Bunun amacý BTreeLeaf.next() içerisinde 3. tryOverflow'agirmesi icin. 2000 veya 2002'yi ararken BTreeLeaf.next() içerisinde 3. tryOverflow'a giriyor. 
			Plan p = new TablePlan("student", tx);
			UpdateScan s = (UpdateScan) p.open();
			for (int i = 0; i < 8; i++) { // 8 tane 2000, 8 tane 2020 girelim.
				s.beforeFirst();
				s.insert();
				s.setInt("gradyear", 2000);
				idx.insert(s.getVal("gradyear"), s.getRid());

				s.insert(); 
				s.setInt("gradyear", 2020);
				idx.insert(s.getVal("gradyear"), s.getRid());
			}
			s.insert();
			s.setInt("gradyear", 2002);
			idx.insert(s.getVal("gradyear"), s.getRid());
			
			s.insert();
			s.setInt("gradyear", 2003);
			idx.insert(s.getVal("gradyear"), s.getRid());
			s.close();
			
			System.out.println( " 8 kayýt sola ve saða Ve 1 tane  ilk LEAF'e kayýt eklemelerden sonra aðacýn son hali");
			System.out.println(idx.getStatistics().toString());  // Aðaç yapýsý :yukseklik=1, 218 kayýt. 1 root, 3 leaf, 35 (1+33+1) OF blocks
		}
		
		idx.resetQueryStats();
		int counter2016 = 0;
		Constant searchkey = new IntConstant(2016);
		idx.beforeFirst(searchkey);
		while (idx.next()) {
			counter2016++;
		}
		System.out.println("POINT QUERY on (student, gradyear):::::::::");
		System.out.println("Point Query (Search Key): " + searchkey);
		System.out.println("--------> Number of Query results: "+ idx.getStatistics().getQueryResults());  // = Number of data
		System.out.println("--------> Number of Disk Access: "+ idx.getStatistics().getReads()+"\n");

		
		idx.resetQueryStats();
		int counter2000 = 0;
		searchkey = new IntConstant(2000);
		idx.beforeFirst(searchkey);
		while (idx.next()) {
			counter2000++;
		}
		System.out.println("POINT QUERY on (student, gradyear):::::::::");
		System.out.println("Point Query (Search Key): " + searchkey);
		System.out.println("--------> Number of Query results: "+ idx.getStatistics().getQueryResults());  // = Number of data
		System.out.println("--------> Number of Disk Access: "+ idx.getStatistics().getReads()+"\n");
		
		
		
		idx.resetQueryStats();
		int counter2020 = 0;
		searchkey = new IntConstant(2020);
		idx.beforeFirst(searchkey);
		while (idx.next()) {
			counter2020++;
		}
		System.out.println("POINT QUERY on (student, gradyear):::::::::");
		System.out.println("Point Query (Search Key): " + searchkey);
		System.out.println("--------> Number of Query results: "+ idx.getStatistics().getQueryResults());  // = Number of data
		System.out.println("--------> Number of Disk Access: "+ idx.getStatistics().getReads()+"\n");
		
		
		
		idx.resetQueryStats();
		int counter2002 = 0;
		searchkey = new IntConstant(2002);
		idx.beforeFirst(searchkey);
		while (idx.next()) {
			counter2002++;
		}
		System.out.println("POINT QUERY on (student, gradyear):::::::::");
		System.out.println("Point Query (Search Key): " + searchkey);
		System.out.println("--------> Number of Query results: "+ idx.getStatistics().getQueryResults());  // = Number of data
		System.out.println("--------> Number of Disk Access: "+ idx.getStatistics().getReads()+"\n");
		
				
		idx.resetQueryStats();
		int counter2003 = 0;
		searchkey = new IntConstant(2003);
		idx.beforeFirst(searchkey);
		while (idx.next()) {
			counter2003++;
		}
		System.out.println("POINT QUERY on (student, gradyear):::::::::");
		System.out.println("Point Query (Search Key): " + searchkey);
		System.out.println("--------> Number of Query results: "+ idx.getStatistics().getQueryResults());  // = Number of data
		System.out.println("--------> Number of Disk Access: "+ idx.getStatistics().getReads()+"\n");

		if (counter2016 != 200 ||counter2000 != 8 || counter2020 != 8 || counter2002 != 1|| counter2003 != 1) // bu student kayýtlarýnýn sayýsý..200 tane gradyear=200 olmasý gerek.
			System.err.println("*****BTree insert errorr");
		
		
		
		/////////////////************    RANGE QUERY    ***************////////////////
		
		System.out.println("RANGE QUERY on (student, gradyear):::::::::");
		Constant[] searchRange = new Constant[]{new IntConstant(2002),new IntConstant(2016)};
		System.out.println("Range Query (Range Keys): [ " + searchRange[0] + ", " + searchRange[1] + " ]");
		
		Plan p = new TablePlan("student", tx);
		UpdateScan s = (UpdateScan) p.open();
		s.beforeFirst();
		List<RID> rids = new ArrayList<RID>();
		while (s.next())
			if (s.getVal("gradyear").compareTo(searchRange[0])==0 || s.getVal("gradyear").compareTo(searchRange[1])==0 ||
					(s.getVal("gradyear").compareTo(searchRange[0])>0 && s.getVal("gradyear").compareTo(searchRange[1])<0 )  )
				rids.add(s.getRid());
		s.beforeFirst();
		
		idx.resetQueryStats();
		int ridcount = 0;
		idx.beforeFirst(searchRange);
		while (idx.next()) {
			RID rid =idx.getDataRid();
			if (!rids.contains(rid))
				System.out.println("*****BTreeTest: rid not inserted.");
			s.moveToRid(rid);
			System.out.println(s.getInt("gradyear"));
			ridcount++;
		}	
		idx.close();
		s.close();
		
		if (ridcount != rids.size())
			System.err.println("*****BTreeTest: not enough rids inserted");
		else {
			System.out.println("Total numbers: " + ridcount);
			System.out.println("--------> Test #: RANGE QUERY PASSES ...");
			System.out.println("--------> Number of Query results: "+ idx.getStatistics().getQueryResults());  // = Number of data
			System.out.println("--------> Number of Disk Access: "+ idx.getStatistics().getReads()+"\n");
		}
	}
	
	
	
	
	
	private static void testDelete() {
		Index idx = new BTreeIndex("btreeIdx", idxsch, tx);
		Plan p = new TablePlan("student", tx);
		UpdateScan s = (UpdateScan) p.open();

		Constant searchkey = new IntConstant(10);
		while (s.next())
			if (s.getVal("sid").equals(searchkey)) {
				idx.delete(searchkey, s.getRid());
				s.delete();
			}
		s.close();

		idx.beforeFirst(searchkey);
		while (idx.next())
			System.out.println("*****BTreeTest: rid not deleted");
		idx.close();
	}

	private static void testIndexSelect() {
		Index idx = new BTreeIndex("btreeIdx", idxsch, tx);
		Plan p = new TablePlan("student", tx);
		TableScan s1 = (TableScan) p.open();
		Constant searchkey = new IntConstant(20);
		Scan s2 = new IndexSelectScan(idx, searchkey, s1);
		List<Constant> vals = new ArrayList<Constant>();
		while (s2.next())
			vals.add(s2.getVal("majorid"));
		s2.close(); // also closes s1 and idx

		int count = 0;
		Scan s3 = p.open();
		while (s3.next())
			if (s3.getVal("sid").equals(searchkey)) {
				count++;
				if (!vals.contains(s3.getVal("majorid")))
					System.out.println("*****BTreeTest: bad index select");
			}
		s3.close();
		if (count != vals.size())
			System.out.println("*****BTreeTest: bad index select count");
	}

	private static void testIndexJoin() {
		Index idx = new BTreeIndex("btreeIdx", idxsch, tx);
		Plan p1 = new TablePlan("student", tx);
		TableScan s1 = (TableScan) p1.open();
		Plan p2 = new TablePlan("dept", tx);
		Scan s2 = p2.open();
		Scan s3 = new IndexJoinScan(s2, idx, "did", s1);

		Plan p4 = new ProductPlan(p1, p2);
		Expression e1 = new FieldNameExpression("majorid");
		Expression e2 = new FieldNameExpression("did");
		Term t = new Term(e1, e2);
		Predicate pred = new Predicate(t);
		Plan p5 = new SelectPlan(p4, pred);
		Scan s5 = p5.open();

		// check to see if s5 is the same as s3
		List<Constant> vals = new ArrayList<Constant>();
		while (s3.next())
			vals.add(s3.getVal("sid"));
		s3.close();

		int count = 0;
		while (s5.next()) {
			count++;
			if (!vals.contains(s5.getVal("sid")))
				System.out.println("*****BTreeTest: bad index join");
		}
		s5.close();
		if (count != vals.size())
			System.out.println("*****BTreeTest: bad index join count");
	}
}
