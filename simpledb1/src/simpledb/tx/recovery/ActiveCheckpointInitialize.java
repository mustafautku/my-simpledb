package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;

/**
 * The PASSIVE_CHECKPOINT log record.
 * @author Edward Sciore
 */
class ActiveCheckpointInitialize implements LogRecord {

   /**
    * Creates a quiescent checkpoint record.
    */
   public ActiveCheckpointInitialize() {}

   /**
    * Creates a log record by reading no other values 
    * from the basic log record.
    * @param rec the basic log record
    */
   public ActiveCheckpointInitialize(BasicLogRecord rec) {}

   /**
    * Writes a checkpoint record to the log.
    * This log record contains the PASSIVE_CHECKPOINT operator,
    * and nothing else.
    * @return the LSN of the last log value
    */
   public int writeToLog() {
      Object[] rec = new Object[] {ACTIVE_CHECKPOINT_INITIATE};
      return logMgr.append(rec);
   }

   public int op() {
      return ACTIVE_CHECKPOINT_INITIATE;
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
      return "<ACTIVE_CHECKPOINT_INITIATE>";
   }
}
