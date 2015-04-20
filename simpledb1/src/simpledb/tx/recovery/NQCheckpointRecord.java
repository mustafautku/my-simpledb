package simpledb.tx.recovery;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import simpledb.log.BasicLogRecord;

/**
 * The CHECKPOINT log record.
 * @author Edward Sciore
 */
public class NQCheckpointRecord implements LogRecord {
//	private List<Integer> txs = new ArrayList<Integer>(); //Active TXs
	private Set<Integer> txs = new HashSet<Integer>(); //Active TXs
	
   /**
    * Creates a NON-quiescent checkpoint record.
    */
	
	public NQCheckpointRecord(Set<Integer> txs) {
	   for (int i : txs)
			this.txs.add(i);

   }
   
   /**
    * Creates a log record by reading no other values 
    * from the basic log record.
    * @param rec the basic log record
    */
   public NQCheckpointRecord(BasicLogRecord rec) {
		int size = rec.nextInt();
		for (int i=0; i<size; i++) 
			txs.add(rec.nextInt());
	}

   
   /** 
    * Writes a checkpoint record to the log.
    * This log record contains the CHECKPOINT operator,
    * and nothing else.
    * @return the LSN of the last log value
    */
   public int writeToLog() {
		int size = txs.size();
		Object[] rec = new Object[size+2];
		rec[0] = NQCHECKPOINT;
		rec[1] = txs.size();
		Iterator<Integer> it=txs.iterator();
		int i=2;
		while(it.hasNext())
			rec[i++] = it.next();
		return logMgr.append(rec);

   }
   
   public int op() {
      return NQCHECKPOINT;
   }
   
   /**
    * Checkpoint records have no associated transaction,
    * and so the method returns a "dummy", negative txid.
    */
   public int txNumber() {
      return -1; // dummy value
   }
   
   /**
    * Does nothing, because a checkpoint record
    * contains no undo information.
    */
   public void undo(int txnum) {}
   
   public String toString() {
	   int size =txs.size();
	   String s="<NQCHECKPOINT ";
	   Iterator<Integer> it=txs.iterator();
	   while (it.hasNext())
			s += (" " + it.next());
	    return s+">";
//	   for(int i=0;i<size;i++)
//		   s += (" " +txs.get(i).toString());	  
   }
   
   public Set<Integer> getActiveTxs(){
	   return txs;
   }
}
