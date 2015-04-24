package simpledb.tx.concurrency;

import simpledb.file.Block;
import simpledb.tx.Transaction;

import java.util.*;
import java.util.concurrent.Semaphore;

/**
 * The concurrency manager for the transaction.
 * Each transaction has its own concurrency manager. 
 * The concurrency manager keeps track of which locks the 
 * transaction currently has, and interacts with the
 * global lock table as needed. 
 * @author Edward Sciore
 */
public class ConcurrencyMgr {
   
   /**
    * The global lock table.  This variable is static because all transactions
    * share the same table.
    */
   private static final ArrayList<Transaction> activeTxList = new ArrayList<Transaction>();
   private static final Map<Transaction, Semaphore> activeTxMutex = new HashMap<Transaction, Semaphore>();
   private static final Semaphore blockingMutex = new Semaphore(1);

   private static LockTable locktbl = new LockTable();
   private Map<Block,String> locks  = new HashMap<Block,String>();

   // Aktif olan tx'lari almak icin burada da transaction objesine bir referans tutmaliyiz.
   private Transaction tx;

   // Transactionun aktif olup olmadigini bu semafordan anlayabiliyoruz.
   // Ornegin, bu transactionun bitmesini bekleyen baska bir transaction, bu semafora bir down yapabilir
   private Semaphore txsem;

   public ConcurrencyMgr(Transaction tx) {
      this.tx = tx;

      testBlockingMutex();
      addTxToActiveList();
   }

   /**
    * Obtains an SLock on the block, if necessary.
    * The method will ask the lock table for an SLock
    * if the transaction currently has no locks on that block.
    * @param blk a reference to the disk block
    */
   public void sLock(Block blk) {
      if (locks.get(blk) == null) {
         locktbl.sLock(blk);
         locks.put(blk, "S");
      }
   }
   
   /**
    * Obtains an XLock on the block, if necessary.
    * If the transaction does not have an XLock on that block,
    * then the method first gets an SLock on that block
    * (if necessary), and then upgrades it to an XLock.
    * @param blk a refrence to the disk block
    */
   public void xLock(Block blk) {
      if (!hasXLock(blk)) {
         sLock(blk);
         locktbl.xLock(blk);
         locks.put(blk, "X");
      }
   }
   
   /**
    * Releases all locks by asking the lock table to
    * unlock each one.
    */
   public void release() {
      for (Block blk : locks.keySet())
         locktbl.unlock(blk);
      locks.clear();
      removeTxFromActiveList();
   }

   public void initiatePassiveCheckpoint() {
      acquireBlockingMutex();
      waitExistingTransactionsToFinish();
   }

   public void finalizePassiveCheckpoint() {
      releaseBlockingMutex();
   }

   public void initiateActiveCheckpoint() {

   }

   public void finalizeActiveCheckpoint() {

   }

   private boolean hasXLock(Block blk) {
      String locktype = locks.get(blk);
      return locktype != null && locktype.equals("X");
   }


   private void addTxToActiveList() {
      try {
         synchronized (activeTxList) {
            activeTxList.add(this.tx);
            txsem = new Semaphore(1);
            txsem.acquire();
            activeTxMutex.put(this.tx, txsem);
         }
      } catch (InterruptedException ex) {
         // Hicbir sey yapma.
      }
   }

   private void removeTxFromActiveList() {
      synchronized (activeTxList) {
         activeTxList.remove(this.tx);
         activeTxMutex.remove(this.tx);
         txsem.release();
      }
   }

   /**
    * Bir transaction baslamak icin bu semaforu test ediyor.
    */
   private void testBlockingMutex() {
      try {
         blockingMutex.acquire();
         blockingMutex.release();
      } catch (InterruptedException ex) {
         // Hicbir sey yapma.
      }
   }

   private void acquireBlockingMutex() {
      try {
         blockingMutex.acquire();
      }  catch (InterruptedException ex) {
         // Hicbir sey yapma.
      }
   }

   private void releaseBlockingMutex() {
      blockingMutex.release();
   }

   /**
    * Aktif transactionlarin mutexlerini lokal bir yere kopyaliyoruz ki, bunun uzerinde is yaparken diger
    * transactionlar beklemesin.
    */
   private HashMap<Transaction, Semaphore> copyActiveTxMutex() {
      HashMap<Transaction, Semaphore> localcopy = new HashMap<Transaction, Semaphore>();

      synchronized (activeTxList) {
            for (Transaction tx : activeTxMutex.keySet()) {
               Semaphore txsem = activeTxMutex.get(tx);
               localcopy.put(tx, txsem);
         }
      }

      return localcopy;
   }

   public void waitExistingTransactionsToFinish() {
      HashMap<Transaction, Semaphore> localActiveTxMutex = copyActiveTxMutex();

      // Hareketin kendi mutexini almasina gerek yok.
      localActiveTxMutex.remove(this.tx);

      for (Semaphore txsem : localActiveTxMutex.values()) {
         try {
            txsem.acquire();
         } catch (InterruptedException ex) {
            // Hicbir sey yapma.
         }
      }
      // Butun mutexleri aldigimiz zaman hepsi bitmis demektir. Bu noktada fonksiyondan cikiyoruz.
   }
}
