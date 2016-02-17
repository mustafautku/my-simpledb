package testextsort;
import java.util.Random;

import simpledb.metadata.MetadataMgr;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.server.SimpleDB;

public class ServerInit2Table {
   public static int  studentmax=9999, deptmax=40; 
   
   public static void initData(String dbdir) {
      System.out.println("BEGIN INITIALIZATION");
      SimpleDB.init(dbdir);
      if (SimpleDB.fileMgr().isNew()) {
         System.out.println("loading data");
         MetadataMgr md = SimpleDB.mdMgr();
         Transaction tx = new Transaction();

         // create and populate the student table
         Schema sch = new Schema();
         sch.addIntField("sid");
         sch.addStringField("sname", 11);
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
            Random _rgen= new Random();
            int _sid=_rgen.nextInt(studentmax);
            rf.setInt("sid", _sid);
            rf.setString("sname", "student"+id);
            rf.setInt("majorid", _rgen.nextInt(deptmax));
//            rf.setInt("majorid", 0);
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
        	 Random _rgen= new Random();
            rf.insert();
//            rf.setInt("did", id);
            rf.setInt("did", _rgen.nextInt(deptmax));
//            rf.setInt("did", 0);
            rf.setString("dname", "dept"+id);
         }
         rf.close();
        
         tx.commit();
         tx = new Transaction();
         tx.recover(); // add a checkpoint record, to limit rollback
      }
   }
}