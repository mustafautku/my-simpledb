package testskyline;

import java.util.Random;

import simpledb.metadata.MetadataMgr;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.server.SimpleDB;

public class InitDoubleData {
   public static int  numberOfTuples=100; 
   
   public static void initData(String dbdir) {
      System.out.println("BEGIN INITIALIZATION");
      SimpleDB.init(dbdir);
      if (SimpleDB.fileMgr().isNew()) {
         System.out.println("loading data");
         MetadataMgr md = SimpleDB.mdMgr();
         Transaction tx = new Transaction();

         // create and populate the student table
         Schema sch = new Schema();
         sch.addDoubleField("a");  // 8B
         sch.addDoubleField("b");  // 8B
         sch.addStringField("c", 80);  // 84B
         md.createTable("input", sch, tx);  // record size: 100B
         TableInfo ti = md.getTableInfo("input", tx);

         RecordFile rf = new RecordFile(ti, tx);
         while (rf.next())
            rf.delete();
         rf.beforeFirst();
         for (int id=0; id<numberOfTuples; id++) {
            rf.insert();
            Random _rgen= new Random();
            double _A=_rgen.nextGaussian();
            rf.setDouble("a", _A);
            
            double _B=_rgen.nextGaussian();
            rf.setDouble("b", _B);
            
            rf.setString("c", "dummy");
         }
         rf.close();

    
         rf.close();
         tx.commit();
         tx = new Transaction();
         tx.recover(); // add a checkpoint record, to limit rollback
      }
   }
}