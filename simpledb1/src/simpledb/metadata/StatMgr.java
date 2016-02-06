package simpledb.metadata;

import simpledb.tx.Transaction;
import simpledb.record.*;
import java.util.*;

/**
 * The statistics manager, which is responsible for
 * keeping statistical information about each table.
 * The manager does not store this information in the database.
 * Instead, it calculates this information on system startup,
 * and periodically refreshes it.
 * @author Edward Sciore
 */

/**
 * setStatInfo is added for loading requested V() stats.
 * @author mustafautku
 *
 */
public class StatMgr {
   private TableMgr tblMgr;
   private Map<String,StatInfo> tablestats;
   private int numcalls;
   
   /**
    * Creates the statistics manager.
    * The initial statistics are calculated by
    * traversing the entire database.
    * @param tx the startup transaction
    */
   public StatMgr(TableMgr tblMgr, Transaction tx) {
      this.tblMgr = tblMgr;
      refreshStatistics(tx);
   }
   
   /**
    * Returns the statistical information about the specified table.
    * @param tblname the name of the table
    * @param ti the table's metadata
    * @param tx the calling transaction
    * @return the statistical information about the table
    */
   
   
   public synchronized StatInfo getStatInfo(String tblname, TableInfo ti, Transaction tx) {
      numcalls++;
//      if (numcalls > 100)    // Increase the update_stat period in order to keep manully-stored field stats (V()s)
      if (numcalls > 500)
         refreshStatistics(tx);
      StatInfo si = tablestats.get(tblname);
      if (si == null) {
         si = calcTableStats(ti,null, tx);
         tablestats.put(tblname, si);
      }
      return si;
   }
   
   // This function stored the requested V() values manually from tests.
	public synchronized void setStatInfo(String tblname, TableInfo ti,
			Map<String, Integer> fieldstats, Transaction tx) {

		StatInfo si = calcTableStats(ti, fieldstats, tx);
		tablestats.put(tblname, si);
	}
   
   private synchronized void refreshStatistics(Transaction tx) {
      tablestats = new HashMap<String,StatInfo>();
      numcalls = 0;
      TableInfo tcatmd = tblMgr.getTableInfo("tblcat", tx);
      RecordFile tcatfile = new RecordFile(tcatmd, tx);
      while(tcatfile.next()) {
         String tblname = tcatfile.getString("tblname");
         TableInfo md = tblMgr.getTableInfo(tblname, tx);
         StatInfo si = calcTableStats(md,null, tx); // ATT.: refresh aldýysa V(.) deðerlerini kaybedidiyoruz. O yüzden numcalls 100 --> 500'e çektim..
         tablestats.put(tblname, si);
      }
      tcatfile.close();
   }
   
   private synchronized StatInfo calcTableStats(TableInfo ti, Map<String,Integer> fieldstats,Transaction tx) {
      int numRecs = 0;
      RecordFile rf = new RecordFile(ti, tx);
      int numblocks = 0;
      while (rf.next()) {
         numRecs++;
         numblocks = rf.currentRid().blockNumber() + 1;
      }
      rf.close();
      return new StatInfo(numblocks, numRecs,fieldstats);
   }
}
