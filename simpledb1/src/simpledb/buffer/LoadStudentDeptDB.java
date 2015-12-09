package simpledb.buffer;

import java.util.HashMap;
import java.util.Map;

import simpledb.file.Page;
import simpledb.metadata.MetadataMgr;
import simpledb.metadata.StatInfo;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.server.SimpleDB;

public class LoadStudentDeptDB {
   public static int studentmax=80, deptmax=7;   //studentmax=45000
//	public static int coursemax=5, studentmax=450, deptmax=4,
//		      sectmax=250, enrollmax=1500;
   

   public static void initData(String dbdir) {
//   public static void main(String[] args) {
      System.out.println("BEGIN INITIALIZATION");
      SimpleDB.init("studentdb");
      if (SimpleDB.fileMgr().isNew()) {
         System.out.println("loading data");
         MetadataMgr md = SimpleDB.mdMgr();
         Transaction tx = new Transaction();

         // create and populate the student table
         Schema sch = new Schema();
         sch.addIntField("sid");
         sch.addStringField("sname", 60);
         sch.addIntField("majorid");
         sch.addIntField("gradyear");
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
            rf.setInt("majorid", id%deptmax);
            rf.setInt("gradyear", (id%50)+1960);
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
         
         tx.commit();
         tx = new Transaction();
         tx.recover(); // add a checkpoint record, to limit rollback
      }
   }

   public static  void showRealDataStatistics(Transaction tx){
	   System.out.println("REAL DATA STATISTICS:");
	   System.out.println("-----------------------");
	   MetadataMgr md = SimpleDB.mdMgr();	   
	   
		
		TableInfo ti = md.getTableInfo("student", tx);
		int numRecs = 0;
		RecordFile rf = new RecordFile(ti, tx);
		int numblocks = 0;
		while (rf.next()) {
			numRecs++;
			numblocks = rf.currentRid().blockNumber() + 1;
		}
		rf.close();
		System.out.println("STUDENT:" + numblocks + " " + numRecs);
		
		ti = md.getTableInfo("dept", tx);
		numRecs = 0;
		rf = new RecordFile(ti, tx);
		numblocks = 0;
		while (rf.next()) {
			numRecs++;
			numblocks = rf.currentRid().blockNumber() + 1;
		}
		rf.close();
		System.out.println("DEPT:" + numblocks + " " + numRecs);
	   
   }
}