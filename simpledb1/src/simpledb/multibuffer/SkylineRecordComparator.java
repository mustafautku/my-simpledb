package simpledb.multibuffer;

import simpledb.query.*;
import java.util.*;

/**
 * A SKYLNE comparator for scans.
 * @author utku kalay
 */
public class SkylineRecordComparator implements Comparator<Scan> {
   private List<String> fields;
   
   /**
    * Creates a comparator using the specified fields,
    * using the ordering implied by its iterator.
    * @param fields a list of field names
    */
   public SkylineRecordComparator(List<String> fields) {
      this.fields = fields;
   }
   
   /**
    * Compares the current records of the two specified scans.
    * The sort fields are considered in turn.
    * When a field is encountered ONLY for which the records have
    * different values, it changes the dominant condition of the scan. 
    * 
    * @param s1 the first scan
    * @param s2 the second scan
    * @return the result of comparing each scan's current record according to the field list
    */
   public int compare(Scan s1, Scan s2) {
	   boolean s1dominant=true;
	   boolean s2dominant=true;
      for (String fldname : fields) {
         Constant val1 = s1.getVal(fldname);
         Constant val2 = s2.getVal(fldname);
         int result = val1.compareTo(val2);
         if (result < 0)
        	 s2dominant=false;  // s2 cannot dominate s1
         else if (result > 0)
        	 s1dominant=false ;  // s1 cannot dominate s2
      }
      if(s1dominant)
    	  return 1;
      else if(s2dominant)
    	  return 2;
      else return 0;  // do not domnate each other.
   }
}
