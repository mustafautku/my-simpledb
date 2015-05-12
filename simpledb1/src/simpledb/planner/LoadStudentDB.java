package simpledb.planner;

import simpledb.metadata.MetadataMgr;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.server.SimpleDB;

public class LoadStudentDB {
   public static int coursemax=300, studentmax=900, deptmax=40,
      sectmax=1200, enrollmax=2000;

   public static void initData(String dbdir) {
//   public static void main(String[] args) {
      System.out.println("BEGIN INITIALIZATION");
      SimpleDB.init("studentdb");
      if (SimpleDB.fileMgr().isNew()) {
         System.out.println("loading data");
         MetadataMgr md = SimpleDB.mdMgr();
         Transaction tx = new Transaction();

         // create and populate the course table
         Schema sch = new Schema();
         sch.addIntField("cid");
         sch.addStringField("title", 20);
         sch.addIntField("deptid");
         md.createTable("course", sch, tx);
         TableInfo ti = md.getTableInfo("course", tx);

         RecordFile rf = new RecordFile(ti, tx);
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

         // create and populate the student table
         sch = new Schema();
         sch.addIntField("sid");
         sch.addStringField("sname", 10);
         sch.addIntField("majorid");
         sch.addIntField("gradyear");
         md.createTable("student", sch, tx);
         ti = md.getTableInfo("student", tx);

         rf = new RecordFile(ti, tx);
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
         sch.addStringField("dname", 8);
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

         // create and populate the section table
         sch = new Schema();
         sch.addIntField("sectid");
         sch.addStringField("prof", 8);
         sch.addIntField("courseid");
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
            int profnum = id%20;
            rf.setString("prof", "prof"+profnum);
            rf.setInt("courseid", id%coursemax);
            rf.setInt("yearoffered", (id%50)+1960);
         }
         rf.close();

         // create and populate the enroll table
         sch = new Schema();
         sch.addIntField("eid");
         sch.addStringField("grade", 2);
         sch.addIntField("studentid");
         sch.addIntField("sectionid");
         md.createTable("enroll", sch, tx);
         ti = md.getTableInfo("enroll", tx);

         rf = new RecordFile(ti, tx);
         while (rf.next())
            rf.delete();
         String[] grades = new String[] {"A", "B", "C", "D", "F"};
         rf.beforeFirst();
         for (int id=0; id<enrollmax; id++) {
            rf.insert();
            rf.setInt("eid", id);
            rf.setString("grade", grades[id%5]);
            rf.setInt("studentid", id%studentmax);
            rf.setInt("sectionid", id%sectmax);
         }
         rf.close();
         tx.commit();
         tx = new Transaction();
         tx.recover(); // add a checkpoint record, to limit rollback
      }
   }
}