package simpledb.tx.recovery;

import static simpledb.tx.recovery.LogRecord.*;
import simpledb.file.Block;
import simpledb.buffer.Buffer;
import simpledb.server.SimpleDB;
import simpledb.tx.LogMan;
import simpledb.tx.concurrency.ConcurrencyMgr;

import java.util.*;
import java.util.logging.Logger;

/**
 * The recovery manager.  Each transaction has its own recovery manager.
 * @author Edward Sciore
 */
public class RecoveryMgr {

   private static int PASSIVE_CKPT = 0;
   private static int ACTIVE_CKPT = 1;

   private static int CKPT_STRATEGY = PASSIVE_CKPT;

   private static int TRANSACTION_PER_CHECKPOINT = 3;
   private static Logger logger = LogMan.getLogger();

   private int txnum;
   private ConcurrencyMgr concurMgr;

   /**
    * Creates a recovery manager for the specified transaction.
    * @param txnum the ID of the specified transaction
    *
    *              Cengiz: Buraya ek olarak concurMgr'i de ekledik.
    */
   public RecoveryMgr(int txnum, ConcurrencyMgr concurMgr) {
      this.txnum = txnum;
      this.concurMgr = concurMgr;

      // Transaction hayatina baslamadan once checkpoint yapayim mi diye bakiyor.
      dealWithCheckpoint();

      new StartRecord(txnum).writeToLog();
   }

   /**
    * Test kodunun bozulmamasi icin bir eski constructoru da buraya koydum.
    * @param txnum
    */
   public RecoveryMgr(int txnum) {
      this.txnum = txnum;
      new StartRecord(txnum).writeToLog();
   }

   /**
    * Writes a commit record to the log, and flushes it to disk.
    */
   public void commit() {
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new CommitRecord(txnum).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   /**
    * Writes a rollback record to the log, and flushes it to disk.
    */
   public void rollback() {
      doRollback();
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new RollbackRecord(txnum).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   /**
    * Recovers uncompleted transactions from the log,
    * then writes a quiescent checkpoint record to the log and flushes it.
    */
   public void recover() {
      doRecover();
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new PassiveCheckpointRecord().writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   /**
    * Writes a setint record to the log, and returns its lsn.
    * Updates to temporary files are not logged; instead, a
    * "dummy" negative lsn is returned.
    * @param buff the buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setInt(Buffer buff, int offset, int newval) {
      int oldval = buff.getInt(offset);
      Block blk = buff.block();
      if (isTempBlock(blk))
         return -1;
      else
         return new SetIntRecord(txnum, blk, offset, oldval).writeToLog();
   }

   /**
    * Writes a setstring record to the log, and returns its lsn.
    * Updates to temporary files are not logged; instead, a
    * "dummy" negative lsn is returned.
    * @param buff the buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setString(Buffer buff, int offset, String newval) {
      String oldval = buff.getString(offset);
      Block blk = buff.block();
      if (isTempBlock(blk))
         return -1;
      else
         return new SetStringRecord(txnum, blk, offset, oldval).writeToLog();
   }

   public void listLog(){
      Iterator<LogRecord> iter = new LogRecordForwardIterator();
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         System.out.println(rec.toString());
      }
   }

   /**
    * Rolls back the transaction.
    * The method iterates through the log records,
    * calling undo() for each log record it finds
    * for the transaction,
    * until it finds the transaction's START record.
    */
   private void doRollback() {
      Iterator<LogRecord> iter = new LogRecordIterator();
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         if (rec.txNumber() == txnum) {
            if (rec.op() == START)
               return;
            rec.undo(txnum);
         }
      }
   }

   /**
    * Does a complete database recovery.
    * The method iterates through the log records.
    * Whenever it finds a log record for an unfinished
    * transaction, it calls undo() on that record.
    * The method stops when it encounters a PASSIVE_CHECKPOINT record
    * or the end of the log.
    */
   private void doRecover() {
      Collection<Integer> finishedTxs = new ArrayList<Integer>();
      Iterator<LogRecord> iter = new LogRecordIterator();
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         if (rec.op() == PASSIVE_CHECKPOINT)
            return;
         if (rec.op() == COMMIT || rec.op() == ROLLBACK)
            finishedTxs.add(rec.txNumber());
         else if (!finishedTxs.contains(rec.txNumber()))
            rec.undo(txnum);
      }
   }

   /**
    * Determines whether a block comes from a temporary file or not.
    */
   private boolean isTempBlock(Block blk) {
      return blk.fileName().startsWith("temp");
   }

   private void dealWithCheckpoint() {
      logger.info("TX " + txnum + " is dealing with checkpoint");
      if (txnum % TRANSACTION_PER_CHECKPOINT == 0) {

         if (CKPT_STRATEGY == PASSIVE_CKPT) {
            doPassiveCheckpoint();
         } else if (CKPT_STRATEGY == ACTIVE_CKPT) {
            doActiveCheckpoint();
         } else {
            // Hicbirsey yapma, sadece log yaz.
            logger.info("CKPT stratejisi duzgun belirlenmemis!!!");
         }

      } else {
         logger.info("TX " + txnum +  " is skipping checkpoint.");
      }
   }

   private void doActiveCheckpoint() {
      int initlsn = new ActiveCheckpointInitialize().writeToLog();
      SimpleDB.logMgr().flush(initlsn);

      concurMgr.waitExistingTransactionsToFinish();

      int finlsn = new ActiveCheckpointFinalize().writeToLog();
      SimpleDB.logMgr().flush(finlsn);
   }

   private void doPassiveCheckpoint() {
      concurMgr.initiatePassiveCheckpoint();

      int lsn = new PassiveCheckpointRecord().writeToLog();
      SimpleDB.logMgr().flush(lsn);

      concurMgr.finalizePassiveCheckpoint();

   }
}
