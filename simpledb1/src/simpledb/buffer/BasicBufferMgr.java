package simpledb.buffer;

import simpledb.file.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;

   private HashMap<String, Buffer> blockBufferHashMap;
   private Queue<Buffer> unpinnedBufferQueue;

   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      blockBufferHashMap = new HashMap<String, Buffer>();
      unpinnedBufferQueue = new LinkedList<Buffer>();

      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++) {
         Buffer newbuf = new Buffer();
         bufferpool[i] = newbuf;
         unpinnedBufferQueue.add(newbuf);
      }
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {

      System.out.println("Attempting to pin block " + blk.toString());

      Buffer buff = findExistingBuffer(blk);
      Block oldblock = null;
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         oldblock = buff.block();  // Block-Buffer eslesmesinden kaldirabilmek icin eski blocku almaliyiz.
         buff.assignToBlock(blk);
      }
      if (!buff.isPinned()) {
         numAvailable--;

         // Buffer-Block kayitlarindaki ekleme-cikarmayi burada yapiyoruz.
         // Unpin'de hicbir islem yapmiyoruz cunku; bir buffer'i unpin ettikten sonra icinde tuttugu blocku hemen cikarmiyoruz
         // Daha sonra bu block icin bir pin istegi gelirse icinde bulunduran buffer'i servis ediyoruz. Boylece
         // hicbir islem yapmadan bir pin istegine cevap verebiliyoruz.
         if (oldblock != null && !buff.block().equals(oldblock)) {
            // Bufferda halihazirda bulunan bir blocktan baska bir blocku buraya aliyoruz.
            // Kayitlarda duran eski blogu kaldirip yenisini koymali.
            System.out.println("Removing block " + oldblock.toString() + " from buffer " + buff.getID() + " on hashmap.");
            blockBufferHashMap.remove(oldblock.toString());

            System.out.println("Putting block " + blk.toString() + " to buffer " + buff.getID() + " on hashmap.");
            blockBufferHashMap.put(blk.toString(), buff);

         }
         else if (oldblock != null && buff.block().equals(oldblock)) {
            // Buradaki durum; bufferi unpin ettik, bu buffera baska hicbir block gelmeden yine ayni block istegi geldi.
            // Hal boyleyken bu block burada zaten varmis deyip baska bir islem yapmiyoruz.
            System.out.println("Pinning previously unpinned buffer because buffer still has the same block.");
         }
         else {
            // Bu baslangic durumu, bufferda hicbir block yok.
            System.out.println("Putting block " + blk.toString() + " to buffer " + buff.getID() + " on hashmap.");
            blockBufferHashMap.put(blk.toString(), buff);
         }
      }

      buff.pin();
      System.out.println(this.toString());
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      System.out.println("Allocating and pinning a new block for file " + filename);
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;

      // assigntonew blocka atanmis bufferi degistiriyor. blockbuffer eslemesinden kaldirmak icin buna bir referans almaliyiz.
      Block oldblock = buff.block();

      buff.assignToNew(filename, fmtr);
      numAvailable--;
      buff.pin();

      if (oldblock != null) {
         System.out.println("Removing block " + oldblock.toString() + " from buffer " + buff.getID() + " on hashmap.");
         blockBufferHashMap.remove(oldblock.toString());
      }

      System.out.println("Putting block " + buff.block().toString() + " to buffer " + buff.getID() + " on hashmap.");
      blockBufferHashMap.put(buff.block().toString(), buff);

      System.out.println(this.toString());
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      System.out.println("Unpinning buffer " + buff.getID());
      buff.unpin();

      if (!buff.isPinned()) {
         numAvailable++;
         unpinnedBufferQueue.add(buff);
      }

      System.out.println(this.toString());

   }

   /**
    * Verilen buffer indisindeki buffer'i unpin eder.
    * Birim testler icin gerekli zira bufferlarin kendisine disaridan ulasamiyoruz.
    */
   synchronized void unpin(int bufferind){
      unpin(bufferpool[bufferind]);
   }

   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
      Buffer existingbuffer = blockBufferHashMap.get(blk.toString());

      if (existingbuffer == null) {
         // Bu blocku tutan bir buffer yok.
         System.out.println("findExistingBuffer is going to return null");
      }
      else {
         // Bu blocku tutan bir buffar var.
         System.out.println("findExistingBuffer is going to return " + existingbuffer.getID());
         if (unpinnedBufferQueue.contains(existingbuffer)) {
            // Bufferi unpin etmisiz, kuyruga koymusuz, ama aradigimiz block burada. Kuyruktan cikaralim
            System.out.println("Removing buffer " + existingbuffer.getID() + " from buffer pool because of unpinned hit.");
            unpinnedBufferQueue.remove(existingbuffer);
         }
      }

      return existingbuffer;
   }
   
   private Buffer chooseUnpinnedBuffer() {
      if (unpinnedBufferQueue.size() == 0) {
         System.out.println("No buffer is available in the queue.");
         return null;
      }
      else {
         Buffer canditate = unpinnedBufferQueue.remove();
         System.out.println("Picked buffer no " + canditate.getID() + " from the queue");
         return canditate;
      }
   }

   /**
    * Bufferlarin durumlarini string seklinde geri dondurur.
    */
   public String toString() {
      String dumpstr = "";
      for (Buffer buff : bufferpool) {
         dumpstr += buff.toString_2() + "\n";
      }

      String queuestatus = "";
      if (unpinnedBufferQueue.size() == 0 ) {
         queuestatus = "Queue is empty";
      } else {
         for (Buffer buff : unpinnedBufferQueue) {
            queuestatus += buff.getID() + " ";
         }
      }
      dumpstr += "Queue status: " + queuestatus + "\n";

      String mapstatus = "Mapping status:\n";
      for (String blkstr : blockBufferHashMap.keySet()) {
         mapstatus += "block " + blkstr + " to buffer " + blockBufferHashMap.get(blkstr).getID() + "\n";
      }
      dumpstr += mapstatus;

      return dumpstr;
   }

}
