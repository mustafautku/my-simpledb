package testqueryexec;

import java.util.HashMap;
import java.util.Map;

import simpledb.metadata.MetadataMgr;
import simpledb.metadata.StatInfo;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.server.SimpleDB;

/*Bu programda Sciori DB kitabýndaki STUDENT VT istatistiksel bilgilerine uygun "studentdb" isimli bir veri tabaný oluþturulmuþtur. 
 * V(.) deðerleri için Stat bilgileri runtime'da oluþturuluyor. Çünkü md bu deðerler için yaklaþýk (1/3) deðer gonderiyor.

STUDENT: B=4500   R=45000    Vsid=45.000       Vsname=45.000     Vgradyear=50  Vmajorid=40
DEPT:    B=2      R=40       Vdid=40           Vdname=40
COURSE:  B=25     R=500      Vcid=500          Vtitle=500        Vdeptid=40
SECTION: B=2500   R=25000    Vsectid=25000     Vcourseid=500     Vprof=250     Vyearoffered=50
ENROLL:  B=50000  R=1500000  Veid=45000		   Vsectionid=25000  Vstudentid=45000  Vgrade=14
*/
public class LoadStudentDB {
//   public static int coursemax=500, studentmax=45000, deptmax=40,
//      sectmax=25000, enrollmax=1500000;
	public static int coursemax=5, studentmax=450, deptmax=4,
		      sectmax=250, enrollmax=1500;
   
   public static void initData(String dbdir) {
//   public static void main(String[] args) {
      System.out.println("BEGIN INITIALIZATION");
      SimpleDB.init(dbdir);
      if (SimpleDB.fileMgr().isNew()) {
         System.out.println("loading data");
         MetadataMgr md = SimpleDB.mdMgr();
         Transaction tx = new Transaction();       

         // create and populate the student table
         Schema sch = new Schema();
         sch.addIntField("sid");
         sch.addStringField("sname", 60);
         sch.addIntField("gradyear");
         sch.addIntField("majorid");
         md.createTable("student", sch, tx);
         TableInfo ti = md.getTableInfo("student", tx);

         RecordFile rf = new RecordFile(ti, tx);
         while (rf.next())
            rf.delete();
         rf.beforeFirst();
         for (int id=0; id<studentmax; id++) {
            rf.insert();
            rf.setInt("sid", id);
            rf.setString("sname", "student"+id);
            rf.setInt("gradyear", (id%50)+1960);
            rf.setInt("majorid", id%deptmax);            
         }
         rf.close();

         // create and populate the dept table
         sch = new Schema();
         sch.addIntField("did");
         sch.addStringField("dname", 28);
         md.createTable("dept", sch, tx);
         ti = md.getTableInfo("dept", tx);

         rf = new RecordFile(ti, tx);
         while (rf.next())
            rf.delete();
         rf.beforeFirst();
         for (int id=0; id<deptmax; id++) {
            rf.insert();
            rf.setInt("did", id);
            rf.setString("dname", "dept"+id);
         }
         rf.close();

         // create and populate the course table
         sch = new Schema();
         sch.addIntField("cid");
         sch.addStringField("title", 24);
         sch.addIntField("deptid");
         md.createTable("course", sch, tx);
         ti = md.getTableInfo("course", tx);

         rf = new RecordFile(ti, tx);
         while (rf.next())
            rf.delete();
         rf.beforeFirst();
         for (int id=0; id<coursemax; id++) {
            rf.insert();
            rf.setInt("cid", id);
            rf.setString("title", "course"+id);
            rf.setInt("deptid", id%deptmax);
         }
         rf.close();
         
         
         // create and populate the section table
         sch = new Schema();
         sch.addIntField("sectid");
         sch.addIntField("courseid");
         sch.addStringField("prof", 60);         
         sch.addIntField("yearoffered");
         md.createTable("section", sch, tx);
         ti = md.getTableInfo("section", tx);

         rf = new RecordFile(ti, tx);
         while (rf.next())
            rf.delete();
         rf.beforeFirst();
         for (int id=0; id<sectmax; id++) {
            rf.insert();
            rf.setInt("sectid", id);
            rf.setInt("courseid", id%coursemax);
            int profnum = id%250;
            rf.setString("prof", "prof"+profnum);            
            rf.setInt("yearoffered", (id%50)+1960);
         }
         rf.close();

         // create and populate the enroll table
         sch = new Schema();
         sch.addIntField("eid");
         sch.addIntField("studentid");
         sch.addIntField("sectionid");
         sch.addStringField("grade", 6);
         md.createTable("enroll", sch, tx);
         ti = md.getTableInfo("enroll", tx);

         rf = new RecordFile(ti, tx);
         while (rf.next())
            rf.delete();
         String[] grades = new String[] {"AA", "BA", "BB", "CB", "CC","DC", "DD", "FD", "FF", "F0","F1", "F2", "F3", "F4"};
         rf.beforeFirst();
         for (int id=0; id<enrollmax; id++) {
            rf.insert();
            rf.setInt("eid", id);
            rf.setInt("studentid", id%studentmax);
            rf.setInt("sectionid", id%sectmax);
            String aGrade=grades[id%14];
            rf.setString("grade",aGrade );            
            System.out.println(id + ": " +aGrade + " eklendi.");
         }
         rf.close();
         tx.commit();
         tx = new Transaction();
         tx.recover(); // add a checkpoint record, to limit rollback
      }
   }
public static void setDataStatistics(Transaction tx) {
	   
	   MetadataMgr md = SimpleDB.mdMgr();
	   
	   TableInfo ti = md.getTableInfo("student", tx);
	   Map<String,Integer> studentFieldstats = new HashMap<String,Integer>();
	   studentFieldstats.put("sid", studentmax);
	   studentFieldstats.put("sname", studentmax);
	   studentFieldstats.put("majorid", deptmax);
	   studentFieldstats.put("gradyear", 50);
	   md.setStatInfo("student", ti, studentFieldstats, tx);
	   
	   ti = md.getTableInfo("dept", tx);
	   Map<String,Integer> deptFieldstats = new HashMap<String,Integer>();
	   deptFieldstats.put("did", deptmax);
	   deptFieldstats.put("dname", deptmax);
	   md.setStatInfo("dept", ti, deptFieldstats, tx);
	   
	   ti = md.getTableInfo("course", tx);
	   Map<String,Integer> courseFieldstats = new HashMap<String,Integer>();
	   courseFieldstats.put("cid", coursemax);
	   courseFieldstats.put("title", coursemax);
	   courseFieldstats.put("deptid", deptmax);
	   md.setStatInfo("course", ti, courseFieldstats, tx);
	   
	   ti = md.getTableInfo("section", tx);
	   Map<String,Integer> sectionFieldstats = new HashMap<String,Integer>();
	   sectionFieldstats.put("sectid", sectmax);
	   sectionFieldstats.put("courseid", coursemax);
	   sectionFieldstats.put("prof", 250);
	   sectionFieldstats.put("yearoffered", 50);
	   md.setStatInfo("section", ti, sectionFieldstats, tx);
	   
	   ti = md.getTableInfo("enroll", tx);
	   Map<String,Integer> enrollFieldstats = new HashMap<String,Integer>();
	   enrollFieldstats.put("eid", enrollmax);
	   enrollFieldstats.put("sectionid", sectmax);
	   enrollFieldstats.put("studentid", studentmax);
	   enrollFieldstats.put("grade", 14);
	   md.setStatInfo("enroll", ti, enrollFieldstats, tx);	   
   }

   public static  void getDataStatistics(Transaction tx){
	   System.out.println("DATA STATISTICS:");
	   System.out.println("-----------------------");
	   MetadataMgr md = SimpleDB.mdMgr();	 	   
		
	   TableInfo ti = md.getTableInfo("student", tx);
	   StatInfo si=md.getStatInfo("student", ti, tx);
	   System.out.println("STUDENT:" +si.blocksAccessed()+ " "+ si.recordsOutput() + " "
					+ si.distinctValues("sid")+ " "+ si.distinctValues("sname")+ " "
					+ si.distinctValues("gradyear")+ " "+ si.distinctValues("majorid"));
		
		ti = md.getTableInfo("dept", tx);
		si=md.getStatInfo("dept", ti, tx);
		System.out.println("DEPT:" +si.blocksAccessed()+ " "+ si.recordsOutput() + " "
				+ si.distinctValues("did")+ " "+ si.distinctValues("dname"));
		
		ti = md.getTableInfo("course", tx);
		si=md.getStatInfo("course", ti, tx);
		System.out.println("COURSE:" +si.blocksAccessed()+ " "+ si.recordsOutput() + " "
				   						+ si.distinctValues("cid")+ " "+ si.distinctValues("title")+ " "
				   						+ si.distinctValues("deptid")+ " ");
		
		ti = md.getTableInfo("section", tx);
		si=md.getStatInfo("section", ti, tx);
		System.out.println("SECTION:" +si.blocksAccessed()+ " "+ si.recordsOutput() + " "
				+ si.distinctValues("sectid")+ " "+ si.distinctValues("courseid")+ " "
				+ si.distinctValues("prof")+ " "+ si.distinctValues("yearoffered"));
		
		ti = md.getTableInfo("enroll", tx);
		si=md.getStatInfo("enroll", ti, tx);
		System.out.println("ENROLL:" +si.blocksAccessed()+ " "+ si.recordsOutput() + " "
				+ si.distinctValues("eid")+ " "+ si.distinctValues("sectionid")+ " "
				+ si.distinctValues("studentid")+ " "+ si.distinctValues("grade"));
		
   }
}
