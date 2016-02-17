package simpledb.query;

import simpledb.record.RID;

/**
 * 
 * @author Edward Sciore
 */
public interface SortScan extends Scan {
   public void savePosition();
   public void restorePosition();
}
