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

/*
 * ACIKLAMA:  Bu test'de PLANLAYICIYI DEBUG ederek anlamak amaclaniyor. Programdaki 3 faklý DEBUG kismini (farklý zamanlarda) 
 * calistirip ayný sonucu verdigini gorun. Bunu icin gerekli bolgeleri comment / uncomment edebilirisniz. 
 * Su anki mevcut 2 planlayýcýnýn calisma ornegi var. BasicPlanner ve HEURISTIC PLANNER. HEURISTIC PLANNER DEBUG 2 'de
 * herhangi bir IDX olmadan calisitiriyor. DEBUG 3'de ise "majorid" IDX uzerinde bir IDX olusturup/yuklandikten sonra 
 * calisiyor.
 * 
 */


public class Test1 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		SimpleDB.init("studentdb");
		LoadStudentDB.initData("studentdb");
		Transaction tx = new Transaction();
		String qry = "select sid, sname, majorid from student, dept "
		   + "where majorid=did and dname='dept0'";
		Parser psr = new Parser(qry);
		QueryData qd = psr.query();
		
		/*   DEBUG 1: BASIC PLANNER  ***/////
		Plan p = new BasicQueryPlanner().createPlan(qd, tx);     
		/*   END OF DEBUG 1: BASIC PLANNER  ***/////
		
		/*   DEBUG 2: HEURISTIC PLANNER without IDX ***/////
//		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);
		/*   END OF DEBUG 2: HEURISTIC PLANNER without IDX  ***/////
		
		/*   DEBUG 3: HEURISTIC PLANNER with MajorID_IDX ***/////
		//Load IDX:
		// first check if the IDX exist:
//		Plan p_student;
//		UpdateScan s_student;
//		Map<String,IndexInfo> indexes  = SimpleDB.mdMgr().getIndexInfo("student", tx);
//		if(!indexes.containsKey("majorid")){
//			Schema idxsch = new Schema();
//			idxsch.addIntField("dataval");
//			idxsch.addIntField("block");
//			idxsch.addIntField("id");
//			Index idx = new BTreeIndex("MajorID_IDX", idxsch, tx);
//			p_student = new TablePlan("student", tx);
//			s_student = (UpdateScan) p_student.open(); // UpdateScan
//																	// is only
//																	// for
//																	// calling
//																	// getRid()
//		System.out.println("loading IDX...");	
//		while (s_student.next()){
//				Constant mojorConstant=s_student.getVal("majorid");
////				int _majorid=(Integer)mojorConstant.asJavaVal();
////				if(_majorid==10)
////					System.out.print("");
////				System.out.println(" inserting "+ _majorid + " to MajorID_IX");  // show loading
//				idx.insert(mojorConstant, s_student.getRid());
//			}
//			s_student.close();			
//			idx.close();
//			SimpleDB.mdMgr().createIndex("MajorID_IDX", "student", "majorid", tx);
//		}
//		
//		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);	
		
		/*   END OF DEBUG 3: HEURISTIC PLANNER with MajorID_IDX ***/////
		
		
		/*
		 * SHOW THE RESULT SET::::
		 */
		Schema sch = p.schema();
		System.out.println("Name\tMajor");
		if (sch.fields().size() != 3
		       || !sch.hasField("sid")
		       || !sch.hasField("sname")
		       || !sch.hasField("majorid"))
		   System.out.println("*****PlannerTest: bad basic plan schema");
		Scan s = p.open();
		while (s.next()) {
			if (s.getInt("majorid") != 0)
				System.out
						.println("*****PlannerTest: bad basic plan selection");
			String sname = s.getString("sname"); // SimpleDB stores field names
			int did = s.getInt("majorid"); // in lower case
			System.out.println(sname + "\t" + did);
		}
		s.close();
		tx.commit();
	}

}

