package simpledb.tx.recovery;

import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

/**
 * The interface implemented by each type of log record.
 * @author Edward Sciore
 */
public interface LogRecord {
   /**
    * The six different types of log record
    */
   static final int
      PASSIVE_CHECKPOINT = 0,
      ACTIVE_CHECKPOINT_INITIATE = 1,
      ACTIVE_CHECKPOINT_FINALIZE = 2,
      START = 3,
      COMMIT = 4,
      ROLLBACK  = 5,
      SETINT = 6,
      SETSTRING = 7;
   
   static final LogMgr logMgr = SimpleDB.logMgr();
   
   /**
    * Writes the record to the log and returns its LSN.
    * @return the LSN of the record in the log
    */
   int writeToLog();
   
   /**
    * Returns the log record's type. 
    * @return the log record's type
    */
   int op();
   
   /**
    * Returns the transaction id stored with
    * the log record.
    * @return the log record's transaction id
    */
   int txNumber();
   
   /**
    * Undoes the operation encoded by this log record.
    * The only log record types for which this method
    * does anything interesting are SETINT and SETSTRING.
    * @param txnum the id of the transaction that is performing the undo.
    */
   void undo(int txnum);
}