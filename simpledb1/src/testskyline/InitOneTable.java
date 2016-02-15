package testskyline;

import java.util.Random;

import simpledb.metadata.MetadataMgr;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.server.SimpleDB;

public class InitOneTable {
   public static int  numberOfTuples=999; 
   
   public static void initData(String dbdir) {
      System.out.println("BEGIN INITIALIZATION");
      SimpleDB.init(dbdir);
      if (SimpleDB.fileMgr().isNew()) {
         System.out.println("loading data");
         MetadataMgr md = SimpleDB.mdMgr();
         Transaction tx = new Transaction();

         // create and populate the student table
         Schema sch = new Schema();
         sch.addIntField("A");
         sch.addIntField("B");
         md.createTable("input", sch, tx);
         TableInfo ti = md.getTableInfo("input", tx);

         RecordFile rf = new RecordFile(ti, tx);
         while (rf.next())
            rf.delete();
         rf.beforeFirst();
         for (int id=0; id<numberOfTuples; id++) {
            rf.insert();
            Random _rgen= new Random();
            int _A=_rgen.nextInt(numberOfTuples);
            rf.setInt("A", _A);
            
            int _B=_rgen.nextInt(numberOfTuples);
            rf.setInt("B", _B);
         }
         rf.close();

    
         rf.close();
         tx.commit();
         tx = new Transaction();
         tx.recover(); // add a checkpoint record, to limit rollback
      }
   }
}