package simpledb.planner;

import java.util.Map;

import simpledb.index.Index;
import simpledb.index.btree.BTreeIndex;
import simpledb.metadata.IndexInfo;
import simpledb.opt.HeuristicQueryPlanner;
import simpledb.parse.Parser;
import simpledb.parse.QueryData;
import simpledb.query.Constant;
import simpledb.query.IntConstant;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/*Bu programda 4 adet sorgunun mevcut HeuristicPlanner ile planlanlanmasý ve ortaya çýkan Plan p, 
 * p.toString() ile ekranda gozukmesi saðlanmýþtýr.   
*/

public class Test2 {
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		LoadStudentDB.initData("studentdb");
		Transaction tx = new Transaction();

		// Q1:	test selectscan
		String Q1 = "select sname from student "
				   + "where sid=100";
		
		//Q2: test indexselect
		String Q2 = "select studentid,sectionid from enroll "
						   + "where studentid=100";
		
		//Q3: test indexjoin
		String Q3 = "select sname,dname from dept,student "
						   + "where majorid=did";
				
		//Q4: test ALL
		String Q4 = "select sname, title from student, enroll, section, course "
				   + "where sid=studentid and sectid=sectionid and cid=courseid and gradyear=2000 and deptid=10";
		
			
		/* HEURISTIC PLANNER with STUDENT.MajorId_IDX,ENROLL.StudentId_IDX, SECTION.SectId_IDX, COURSE.CId_IDX ***/////
		//Load IDX if NOT exist:
		System.out.println("\nINDEX COORDINATION:");
		System.out.println("----------------------");
		loadIndex("student","majorid","MajorID_IDX",tx);
		loadIndex("enroll","studentid","StudentId_IDX",tx);
		loadIndex("section","sectid","SectId_IDX",tx);
		loadIndex("course","cid","CId_IDX",tx);
		
		System.out.println("----------------------");
		System.out.println("Q1:" + Q1);		
		Plan p1=showQueryPlan(Q1,tx);
		System.out.println("RESULT SET:");		
		Scan s = p1.open();
		while (s.next()) {
			//Q1:
			String sname = s.getString("sname");// SimpleDB stores field names
			System.out.print(sname + "\t");
			System.out.println("");
		}
		s.close();		
		System.out.println("----------------------");
		
		System.out.println("Q2:"+ Q2);
		Plan p2=showQueryPlan(Q2,tx);
		System.out.println("RESULT SET:");
		s = p2.open();
		while (s.next()) {			
			//Q2:
			int sid = s.getInt("studentid"); // SimpleDB stores field names
			int sectid = s.getInt("sectionid"); // in lower case
			System.out.print(sid + "\t");
			System.out.print(sectid);
			System.out.println("");
		}
		s.close();		
		System.out.println("----------------------");
		
		System.out.println("Q3:"+Q3);
		Plan p3=showQueryPlan(Q3,tx);
		System.out.println("RESULT SET:");
		s = p3.open();
		while (s.next()) {			
			//Q3:
			String sname = s.getString("sname"); // SimpleDB stores field names
			String dname = s.getString("dname"); // in lower case
			System.out.print(sname + "\t");
			System.out.print(dname);
			System.out.println("");
		}
		s.close();
		System.out.println("----------------------");
		
		System.out.println("Q4:" + Q4);		
		Plan p5=showQueryPlan(Q4,tx);		
		System.out.println("RESULT SET:");
		s = p5.open();
		while (s.next()) {								
			//Q5:
			String sname = s.getString("sname"); // SimpleDB stores field names
			String title = s.getString("title"); // in lower case
			System.out.print(sname + "\t");
			System.out.print(title);
			System.out.println("");
		}
		s.close();
		tx.commit();
	}
	
	private static  Plan showQueryPlan(String Q, Transaction tx){
		Parser psr = new Parser(Q);
		QueryData qd = psr.query();
		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);	
		String resultPlan=p.toString();
		System.out.println("OUTPUT PLAN:   " + resultPlan);
		return p;
	}
	
	private static void loadIndex(String TABLEname,String FIELDname,String IDXname,Transaction tx){
		Map<String,IndexInfo> indexes  = SimpleDB.mdMgr().getIndexInfo(TABLEname, tx);
		if(!indexes.containsKey(FIELDname)){
			Plan p = new TablePlan(TABLEname, tx);
			UpdateScan s = (UpdateScan) p.open(); // UpdateScan is only for
													// getRID
			Schema idxsch = new Schema();
			idxsch.addIntField("dataval");
			idxsch.addIntField("block");
			idxsch.addIntField("id");
			Index idx = new BTreeIndex(IDXname, idxsch, tx);

			System.out.println("loading "+IDXname + "...");
			while (s.next()) {
				idx.insert(s.getVal(FIELDname), s.getRid());
			}
			s.close();
			idx.close();
			SimpleDB.mdMgr().createIndex(IDXname, TABLEname, FIELDname, tx);
		}
		else
			System.out.println(IDXname + " already exist...");
	}

}

