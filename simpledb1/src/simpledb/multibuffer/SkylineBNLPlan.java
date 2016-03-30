package simpledb.multibuffer;

import java.util.List;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.materialize.*;
import simpledb.query.*;

/**
 * The Plan class for the muti-buffer version of the
 * <i>SKYLINE</i> operator.
 * @author Edward Sciore
 */
public class SkylineBNLPlan implements Plan {
   private Plan srcplan;
   private Transaction tx;
   private Schema sch;
   private List<String> skylinefields;

   
   /**
    * Creates a product plan for the specified queries.
    * @param lhs the plan for the LHS query
    * @param rhs the plan for the RHS query
    * @param tx the calling transaction
    */
	public SkylineBNLPlan(Plan srcplan, List<String> skylinefields,
			Transaction tx) {
		this.srcplan = srcplan;
		this.tx = tx;
		sch = srcplan.schema();
		this.skylinefields=skylinefields;
	}
   
   /**
    * A scan for this query is created and returned, as follows.
    * First, 
    * It determines the optimal chunk size,
    * based on the the
    * number of available buffers.
    * It creates a chunk plan for each chunk, saving them in a list.
    * Finally, it creates a multiscan for this list of plans,
    * and returns that scan.
    * @see simpledb.query.Plan#open()
    */
   public Scan open() {
	   // input source:  uses 1 buffer
	   Scan input = srcplan.open();
	   // output file (generated at the end of iteration): uses 2 buffers (including for appending)
	   TempTable outputfile = new TempTable(sch, tx);
	   TableScan output = (TableScan) outputfile.open();
		
	   ChunkScan window = new ChunkScan(SimpleDB.BUFFER_SIZE - 3, sch, tx);
	   return new SkylineBNLScan(input,window,output, sch,skylinefields,tx);
   }
   
   /**
    * Returns an estimate of the number of block accesses
    * required to execute the query. The formula is:
    * <pre> B(product(p1,p2)) = B(p2) + B(p1)*C(p2) </pre>
    * where C(p2) is the number of chunks of p2.
    * The method uses the current number of available buffers
    * to calculate C(p2), and so this value may differ
    * when the query scan is opened.
    * @see simpledb.query.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      // this guesses at the # of chunks
//      int avail = SimpleDB.bufferMgr().available();
//      int size = new MaterializePlan(rhs, tx).blocksAccessed();
//      int numchunks = size / avail;
//      return rhs.blocksAccessed() +
//         (lhs.blocksAccessed() * numchunks);
	   return 0;
   }
   
   /**
    * Estimates the number of output records in the product.
    * The formula is:
    * <pre> R(product(p1,p2)) = R(p1)*R(p2) </pre>
    * @see simpledb.query.Plan#recordsOutput()
    */
   public int recordsOutput() {
//      return lhs.recordsOutput() * rhs.recordsOutput();
	   return 0;
   }
   
   /**
    * Estimates the distinct number of field values in the product.
    * Since the product does not increase or decrease field values,
    * the estimate is the same as in the appropriate underlying query.
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
//      if (lhs.schema().hasField(fldname))
//         return lhs.distinctValues(fldname);
//      else
//         return rhs.distinctValues(fldname);
	   return 0;
   }
   
   /**
    * Returns the schema of the product,
    * which is the union of the schemas of the underlying queries.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }

}
