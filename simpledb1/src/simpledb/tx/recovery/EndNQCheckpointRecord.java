package simpledb.tx.recovery;

import java.util.ArrayList;
import java.util.List;

import simpledb.log.BasicLogRecord;

/**
 * The CHECKPOINT log record.
 * @author Edward Sciore
 */
public class EndNQCheckpointRecord implements LogRecord {
	
   /**
    * Creates a END  NON-quiescent checkpoint record.
    */
	
	public EndNQCheckpointRecord() {}
   
   /**
    * Creates a log record by reading no other values 
    * from the basic log record.
    * @param rec the basic log record
    */
   public EndNQCheckpointRecord(BasicLogRecord rec) {}

   
   /** 
    * Writes a checkpoint record to the log.
    * This log record contains the CHECKPOINT operator,
    * and nothing else.
    * @return the LSN of the last log value
    */
   public int writeToLog() {
	   Object[] rec = new Object[] {ENDNQCHECKPOINT};
		return logMgr.append(rec);

   }
   
   public int op() {
      return ENDNQCHECKPOINT;
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
	   
	   String s="<END NQCKPT>";
	 
	   return s;
   }
}
