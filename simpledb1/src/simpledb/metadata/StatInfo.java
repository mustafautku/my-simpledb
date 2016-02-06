package simpledb.metadata;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds three pieces of statistical information about a table:
 * the number of blocks, the number of records,
 * and the number of distinct values for each field. 
 * @author Edward Sciore
 */

/***
 * distinctValues function has been changed. 
 * fieldstats has been added. StatInfo constructor is changed to load fieldstats.
 * @author mustafautku
 *
 */
public class StatInfo {
   private int numBlocks;
   private int numRecs;
   private Map<String,Integer> fieldstats = new HashMap<String,Integer>();
   
   /**
    * Creates a StatInfo object.
    * Note that the number of distinct values is not
    * passed into the constructor.
    * The object fakes this value.
    * @param numblocks the number of blocks in the table
    * @param numrecs the number of records in the table
    */
//   public StatInfo(int numblocks, int numrecs) {
//	      this.numBlocks = numblocks;
//	      this.numRecs   = numrecs;
//	   }
   
   
   // Added for storing field stats-V(.) values.
   public StatInfo(int numblocks, int numrecs, Map<String,Integer> fieldstats) {
      this.numBlocks = numblocks;
      this.numRecs   = numrecs;
      this.fieldstats=fieldstats;
   }
   
   /**
    * Returns the estimated number of blocks in the table.
    * @return the estimated number of blocks in the table
    */
   public int blocksAccessed() {
      return numBlocks;
   }
   
   /**
    * Returns the estimated number of records in the table.
    * @return the estimated number of records in the table
    */
   public int recordsOutput() {
      return numRecs;
   }
   
   /**
    * Returns the estimated number of distinct values
    * for the specified field.
    * In actuality, this estimate is a complete guess.
    * @param fldname the name of the field
    * @return a guess as to the number of distinct field values
    */
//   public int distinctValues(String fldname) {
//      return 1 + (numRecs / 3);
//   }
   
   // added for setting field stats (distributions) manually.
   public int distinctValues(String fldname) {
     return (fieldstats==null) ? (1 + (numRecs / 3)):(Integer)fieldstats.get(fldname).intValue();
  }
   
   
}
