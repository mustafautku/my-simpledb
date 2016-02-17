package testextsort;

import simpledb.materialize.MergeJoinPlan;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class MergeJoinTest1 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ServerInit2Table.initData("testExtSort");
		
		SimpleDB.ExtSortParameters.splitK = 1;
		SimpleDB.ExtSortParameters.mergeK = 4;  // 1:old version 2 way merge (DO NOT USE IT. It has bug at restore())
		
		
		Transaction tx = new Transaction();
		System.out.println("**********MERGE-JOIN TEST *************");
		System.out.println("------------------------------");
		System.out.println("SPLIT K: " + SimpleDB.ExtSortParameters.splitK);
		System.out.println("Merge K: " + SimpleDB.ExtSortParameters.mergeK);
		System.out.println("RepSel K: " + SimpleDB.ExtSortParameters.repSelK);
		
		Plan p1 = new TablePlan("dept", tx);
		Plan p2 = new TablePlan("student", tx);
		Plan mjp =null;
		Scan mjs =null;
		
		int mergejoincount = 0;
		int prevmergejoincount=0;
		int previd=-1;
		
		mjp = new MergeJoinPlan(p1, p2, "did", "majorid", tx); // restore
																	// is not
																	// needed.
																	// Easy.
		mjs = mjp.open();
		while (mjs.next()) {
			mergejoincount++;
			
			int sid = mjs.getInt("sid");
			int majorid = mjs.getInt("majorid");
			int did = mjs.getInt("did");
			
			if (majorid != did)
				System.err.println("merge join FAIL!!!");
			if (sid != previd) {
				System.out.println(sid + " " + majorid + " " + did + " "
						+ (mergejoincount - prevmergejoincount));
				prevmergejoincount = mergejoincount;
			}
			previd = sid;
		}
		mjs.close();
		System.out.println("merge join SUCCESS!!!: Total " + mergejoincount + " matches");

		
		mergejoincount = 0;
		prevmergejoincount=0;
		previd=-1;
		
		mjp = new MergeJoinPlan(p2, p1, "majorid", "did", tx);// restore is
																	// required.
																	// Diffucult.
		mjs = mjp.open();		
		while (mjs.next()) {
			mergejoincount++;
			
			int sid = mjs.getInt("sid");
			int majorid = mjs.getInt("majorid");
			int did = mjs.getInt("did");


			if (majorid != did)
				System.err.println("merge join FAIL!!!");
			if (sid != previd){
				System.out.println(sid + " " + majorid + " " + did  + " " + (mergejoincount-prevmergejoincount));
				prevmergejoincount=mergejoincount;
			}
			previd=sid;
		}
		mjs.close();

		System.out.println("merge join SUCCESS!!!: Total " + mergejoincount + " matches");

		tx.commit();
	}

}
