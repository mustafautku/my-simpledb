package testqueryexec;

import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.server.SimpleDB.PlannerType;
import simpledb.tx.Transaction;
import simpledb.index.Index;
import simpledb.index.btree.BTreeIndex;
import simpledb.index.hash.HashIndex;
import simpledb.parse.*;
import simpledb.planner.BasicQueryPlanner;
import simpledb.planner.BasicUpdatePlanner;
import simpledb.planner.Planner;

import simpledb.query.*;

/**
 * Index coordination test cases. Similiar test formats for each test. Attention to tx.rollback
 * at the end of UPDATE tests. Also attention to remove idx file from file system manually.
 * @author mustafautku
 *
 */
public class QTest3 {
	public static void main(String[] args) {
		String dbname = "studentdb";
		LoadStudentDB.initData(dbname);

		Transaction tx = new Transaction();
		LoadStudentDB.setDataStatistics(tx); // set the field stats according to
												// sciori's book.
		LoadStudentDB.getDataStatistics(tx);
		tx.commit();
//		testQuery();
		
		////**** TEST INDEX COORDINATION. ADD(LOAD)/DROP INDEXES
		testCrDrIndex();  // tx.commit, thus stores into the catalog. Next function will remove those entries.
//		testDrIndex();  // only remove from catalog entries if called after testCrIndex. For complete removal, it should be called seperately.
						// Thus, check file system and remove idx files that is intended to romove manually

//      testCrManyIndex(); // tx.commit, thus stores into the catalog. Next function will remove those entries.
//		testDrIndexAll();  // This function only removes idx entriesin the catalog. Remove from file system manually.
		
//		 testInsert();
//		 testDelete();
//		 testModify();
		
		//ATT: After functions below,Remove idx files from file system manually. Tx.rollback only removes from catalog.
//		testInsertWithIndex(); 
//		testDeleteWithIndex();
//		testModifyWithIndex();
		
//		testJoinQueryWithIndex();
	}

	private static void testQuery() {
		Transaction tx = new Transaction();
		String qry = "select sid, sname, majorid from student, dept "
				+ "where majorid=did and dname='dept0'";
		Parser psr = new Parser(qry);
		QueryData qd = psr.query();
		Plan p = new BasicQueryPlanner().createPlan(qd, tx);

		Scan s = p.open();
		while (s.next())
			System.out.println(s.getString("sname") + " " + s.getString("majorid"));
		s.close();
		tx.commit();
	}
	private static void testCrDrIndex(){
		Transaction tx = new Transaction();
		String cidx="create index majoridbtreeidx on student(majorid)";
		Planner planner=SimpleDB.planner(PlannerType.BASIC); //HEURISTIC yapsan da farketmez. IndexUpdatePlanner'da tek kayýt insert'lerde idx kullanýyor.
		int success=planner.executeUpdate(cidx, tx);
		if(success>0)  // ok. the catalog did not have same kind of index. We entered required INDEXINFO into catalog. Now, we will load the index. 
			loadIndex("student","majorid",
			"majoridbtreeidx", "btree", tx);
		
		// Ayný tx'da drop yapalim. Katalog dan siliyor. FAkat her nasilsa File System'den silemiyor. 
		String didx="drop index majoridbtreeidx on student(majorid)";
		planner=SimpleDB.planner(PlannerType.BASIC);
		success=planner.executeUpdate(didx, tx);
		if(success>0)
			System.out.println(success + " idx is deleted from catalog. CHECK File Mgr. Delete manually if necessary.");
		
		tx.commit();
	}
	private static void testDrIndex(){
		Transaction tx = new Transaction();
		String didx="drop index majoridbtreeidx on student(majorid)";
		Planner planner=SimpleDB.planner(PlannerType.BASIC);
		int success=planner.executeUpdate(didx, tx);
		if(success>0)
			System.out.println(success + " idx is deleted from catalog. CHECK File Mgr. Delete manually if necessary.");
		tx.commit();
	}
	private static void testCrManyIndex(){
		Transaction tx = new Transaction();
		Planner planner=SimpleDB.planner(PlannerType.BASIC);
		
		String cidx="create index majoridbtreeidx on student(majorid)";
		int success=planner.executeUpdate(cidx, tx);
		if(success>0)  // ok. the catalog did not have same kind of index. We entered required INDEXINFO into catalog. Now, we will load the index. 
			loadIndex("student","majorid",
			"majoridbtreeidx", "btree", tx);
		
		cidx="create shash index sidshashidx on student(sid)";
		success=planner.executeUpdate(cidx, tx);
		if(success>0)  // ok. the catalog did not have same kind of index. We entered required INDEXINFO into catalog. Now, we will load the index. 
			loadIndex("student","sid",
			"sidshashidx", "shash", tx);
		
		cidx="create btree index sidbtreeidx on student(sid)";
		success=planner.executeUpdate(cidx, tx);
		if(success>0)  // ok. the catalog did not have same kind of index. We entered required INDEXINFO into catalog. Now, we will load the index. 
			loadIndex("student","sid",
			"sidbtreeidx", "btree", tx);
		
		cidx="create btree index eidbtreeidx on enroll(eid)";
		success=planner.executeUpdate(cidx, tx);
		if(success>0)  // ok. the catalog did not have same kind of index. We entered required INDEXINFO into catalog. Now, we will load the index. 
			loadIndex("enroll","eid",
			"eidbtreeidx", "btree", tx);
		
		tx.commit();
	}
	private static void testDrIndexAll(){  
		Transaction tx = new Transaction();
		String didx="drop indexall on student"; // This prints "3 idx are deleted....". Because we drop indexes in the STUDENT. there are 3 idx over it.
		Planner planner=SimpleDB.planner(PlannerType.BASIC);
		int success=planner.executeUpdate(didx, tx);
		if(success>0)
			System.out.println(success + " idxs are deleted from catalog. CHECK File Mgr. MUST Delete ALL idxs manually");
		// we have 1 more idx on enroll. 
		didx="drop indexall on enroll"; // This prints "3 idx are deleted....". Because we drop indexes in the STUDENT. there are 3 idx over it.
		planner=SimpleDB.planner(PlannerType.BASIC);
		success=planner.executeUpdate(didx, tx);
		if(success>0)
			System.out.println(success + " idxs are deleted from catalog. CHECK File Mgr. MUST Delete ALL idxs manually");
		
		didx="drop indexall on dept"; // This prints "3 idx are deleted....". Because we drop indexes in the STUDENT. there are 3 idx over it.
		planner=SimpleDB.planner(PlannerType.BASIC);
		success=planner.executeUpdate(didx, tx);
		if(success>0)
			System.out.println(success + " idxs are deleted from catalog. CHECK File Mgr. MUST Delete ALL idxs manually");
		tx.commit();
	}
	
	
	
	private static void testInsert() {
		Transaction tx = new Transaction();
		Planner planner=SimpleDB.planner(PlannerType.BASIC); 
		String cmd = "insert into dept(did,dname) values(-1, 'basketry')";
		int success=planner.executeUpdate(cmd, tx);
		
		if (success != 1)
			System.out.println("*****PlannerTest: bad insertion return value");

		String qry = "select did from dept where dname='basketry'";
		Plan p=planner.createQueryPlan(qry, tx);
		
		Scan s = p.open();
		int selectcount = 0;
		while (s.next()) {
			if (s.getInt("did") != -1)
				System.out.println("*****PlannerTest: bad insert retrieval");
			selectcount++;
		}
		if (selectcount != 1)
			System.out.println("*****PlannerTest: bad insert count");
		tx.rollback();
	}
	
	private static void testDelete() {
		Transaction tx = new Transaction();
		String cmd = "delete from dept where did = 1";  
		Planner planner=SimpleDB.planner(PlannerType.BASIC);

		int success=planner.executeUpdate(cmd, tx);
		System.out.println(success + " records are deleted");  //prints 1 records are deleted
		tx.rollback();
	}

	private static void testModify() {
		Transaction tx = new Transaction();
		String cmd = "update dept set did = -1 where did = 1";
		Planner planner=SimpleDB.planner(PlannerType.BASIC);
		planner.executeUpdate(cmd, tx);
		String qry = "select did from dept where did = -1";
		Plan p=planner.createQueryPlan(qry, tx);
		Scan s = p.open();
		int count = 0;
		while (s.next())
			count++;
		s.close();
		if (count != 1)
			System.out.println("*****PlannerTest: wrong records modified");

		tx.rollback();
	}
	
	
	private static void testInsertWithIndex() {
		Transaction tx = new Transaction();
		String cidx="create index didbtreeidx on dept(did)";
		Planner planner=SimpleDB.planner(PlannerType.HEURISTIC); // BASIC de yapsak bulk load da farketmez. Ancak ilerideki tek tek insert iþlemlernde INDEXUPDATE kullanmak için simdiden HEURISTIC yaptýk.
		int success=planner.executeUpdate(cidx, tx);
		if(success>0)  // ok. the catalog did not have same kind of index. We entered required INDEXINFO into catalog. Now, we will load the index. 
			loadIndex("dept","did",
			"didbtreeidx", "btree", tx);
//		tx.commit();
		
		// insert 3 records with indexupdate
		String cmd = "insert into dept(did,dname) values(-1, 'basketry')";
		success=planner.executeUpdate(cmd, tx);
		planner.executeUpdate(cmd, tx);
		planner.executeUpdate(cmd, tx);
		
		if (success != 1)
			System.out.println("*****PlannerTest: bad insertion return value");

//		tx.commit();
		
		// use index search by using heuristic planner
		String qry = "select did from dept where did=-1";
		Plan p=planner.createQueryPlan(qry, tx);
		
		Scan s = p.open();
		int selectcount = 0;
		while (s.next()) {
			if (s.getInt("did") != -1)
				System.out.println("*****PlannerTest: bad insert retrieval");
			selectcount++;
		}
		if (selectcount != 3)  // TOTAL 3 records were inserted. Index should find all.
			System.out.println("*****PlannerTest: bad insert count");
		tx.rollback(); // This removes all updates including nex index info in the catalog. However you still need to remove indexes from file sisyem manually.
	}
	
	private static void	testDeleteWithIndex(){
		Transaction tx = new Transaction();
		String cidx="create shash index sidshashidx on student(sid)";
		Planner planner=SimpleDB.planner(PlannerType.HEURISTIC); // BASIC de yapsak bulk load da farketmez. Ancak ilerideki tek tek insert iþlemlernde INDEXUPDATE kullanmak için simdiden HEURISTIC yaptýk.
		int success=planner.executeUpdate(cidx, tx);
		if(success>0)  // ok. the catalog did not have same kind of index. We entered required INDEXINFO into catalog. Now, we will load the index. 
			loadIndex("student","sid",
			"sidshashidx", "shash", tx);
//		tx.commit();
		
		// insert 2 more records with sid=1
		String cmd1 = "insert into student(sid,sname) values(1, 'utku')";
		String cmd2 = "delete from student where sid = 1";
		success=planner.executeUpdate(cmd1, tx);   //there are 2 students with sid=1 also in the idx
		success=planner.executeUpdate(cmd1, tx);   //there are 3 students with sid=1 also in the idx
		success=planner.executeUpdate(cmd2, tx);   // this command is expected to delete 3 records (with using idx)
		
		if (success != 3)  // 3 records are deleted.
			System.out.println("*****PlannerTest: bad delete return value");

//		tx.commit();
		
		// use index search by using heuristic planner
		String qry = "select sid from student where sid=1";
		Plan p=planner.createQueryPlan(qry, tx);
		
		Scan s = p.open();
	
		if(s.next())
			System.out.println("*****PlannerTest: bad delete");
		tx.rollback(); // This removes all updates including nex index info in the catalog. However you still need to remove indexes from file sisyem manually.
	}
	private static void testModifyWithIndex(){
		Transaction tx = new Transaction();
		String cidx="create shash index sidshashidx on student(sid)";
		Planner planner=SimpleDB.planner(PlannerType.HEURISTIC); // BASIC de yapsak bulk load da farketmez. Ancak ilerideki tek tek insert iþlemlernde INDEXUPDATE kullanmak için simdiden HEURISTIC yaptýk.
		int success=planner.executeUpdate(cidx, tx);
		if(success>0)  // ok. the catalog did not have same kind of index. We entered required INDEXINFO into catalog. Now, we will load the index. 
			loadIndex("student","sid",
			"sidshashidx", "shash", tx);
//		tx.commit();
		
		String cmd1 = "insert into student(sid,sname) values(1, 'utku')";
		String cmd2 = "update student set sid=600 where sid=1";
		
		success=planner.executeUpdate(cmd1, tx);   //there are 2 students with sid=1 also in the idx
		success=planner.executeUpdate(cmd1, tx);   //there are 3 students with sid=1 also in the idx
		success=planner.executeUpdate(cmd2, tx);   // this command is expected to update 3 records (with using idx)
		
		if (success != 3)  // 3 records are updated.
			System.out.println("*****PlannerTest: bad delete return value");

//		tx.commit();
		
		// use index search by using heuristic planner
		String qry = "select sid from student where sid=1";
		Plan p=planner.createQueryPlan(qry, tx);
		
		Scan s = p.open();
	
		if(s.next())
			System.out.println("*****PlannerTest: bad delete");
		
		String qry2 = "select sid from student where sid=600";
		p=planner.createQueryPlan(qry2, tx);
		
		s = p.open();
	
		int selectcount = 0;
		while (s.next()) {
			if (s.getInt("sid") != 600)
				System.out.println("*****PlannerTest: bad insert retrieval");
			selectcount++;
		}
		if (selectcount != 3)  // TOTAL 3 records were inserted. Index should find all.
			System.out.println("*****PlannerTest: bad insert count");
			
		tx.rollback(); // This removes all updates including nex index info in the catalog. However you still need to remove indexes from file sisyem manually.

	}
private static void testJoinQueryWithIndex() {
		
		Transaction tx = new Transaction();		
		String cidx="create index majoridbtreeidx on student(majorid)";
		Planner planner=SimpleDB.planner(PlannerType.HEURISTIC);
		int success=planner.executeUpdate(cidx, tx);
		if(success>0)  // ok. the catalog did not have same kind of index. We entered required INDEXINFO into catalog. Now, we will load the index. 
			loadIndex("student","majorid",
			"majoridbtreeidx", "btree", tx);
		
		// use IDXJOIN(dept,student,majorididx)
		String qry = "select sname, majorid from student,dept "
				+ "where did=majorid and did=2";
		Plan p = planner.createQueryPlan(qry, tx);

		Scan s = p.open();
		while (s.next())
			System.out.println(s.getString("sname") + " " + s.getInt("majorid"));
		s.close();
		tx.rollback();
	}


	/*
	 * LOAD DATA: Neither BasicUpdatePlanner nor IndexUpdatePlanner has the capability of loading whole data into IDX.
	 * Thus this function scans the data file and inserts each record into the index defined in the arguman list. 
	 */
	private static void loadIndex(String TABLEname, String FIELDname,
			String IDXname, String indextype, Transaction tx) {
		
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


	}
}
