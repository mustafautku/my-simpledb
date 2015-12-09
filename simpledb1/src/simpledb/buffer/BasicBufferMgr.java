package simpledb.buffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;

import simpledb.file.*;
import simpledb.server.SimpleDB;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   
   private Map<Block,Buffer> bufferpool;
   private int numAvailable,numBuffs;
   private Queue<Buffer> unpinned=new LinkedList<Buffer>();
   private Stack<Buffer> unpinned_mru=new Stack<Buffer>();
   
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

      bufferpool= new HashMap<Block,Buffer> (numbuffs,1); // load factor is set to 1.0
      numBuffs=numbuffs;
      numAvailable = numbuffs;
      
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      Iterable<Buffer> buffers=bufferpool.values();
      Iterator<Buffer> it=buffers.iterator();
      while(it.hasNext()){
    	  Buffer buff=it.next();
    	  if (buff.isModifiedBy(txnum))
    	         buff.flush();
      }
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
      Buffer buff = findExistingBuffer(blk);
      
      if (buff == null) {
    	  if(bufferpool.size() < numBuffs) {  // thus MaP is not full
        	  buff=new Buffer(bufferpool.size());
        	  buff.assignToBlock(blk);
        	  bufferpool.put(blk, buff);
        	  numAvailable--;
        	  buff.pin();
              return buff;
    	   }
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         
         Buffer oldBuffer=bufferpool.remove(buff.block());    // oldBuffer = buff
         buff.assignToBlock(blk);
         bufferpool.put(blk, buff);         
      }
      if (!buff.isPinned()){
         numAvailable--;
         unpinned.remove(buff); //findExistingBuffer'den gelen 'unpin' bir buffer ise, unpin listesinden cýkarmak gerek.
         unpinned_mru.remove(buff);
      }
      buff.pin();
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
	   Buffer buff=null;
	   Block blk=null;
	   if(bufferpool.size() < numBuffs) {  // thus MaP is not full
     	  buff=new Buffer(bufferpool.size());
     	  blk=buff.assignToNew(filename, fmtr);
     	  bufferpool.put(blk, buff);
     	  numAvailable--;
     	  buff.pin();
          return buff;
 	   }
      buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      
      Buffer oldBuffer=bufferpool.remove(buff.block());      
      blk=buff.assignToNew(filename, fmtr);
      
      bufferpool.put(blk, buff);
      
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()){
         numAvailable++;
         String buffer_policy=SimpleDB.BUFFER_REPLACEMENT_POLICY;
         if(buffer_policy=="lru")
        		 unpinned.add(buff); // add to end of list
         else if(buffer_policy=="mru")
        	 	unpinned_mru.push(buff);
      }
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   
   
	private Buffer findExistingBuffer(Block blk) {
		return bufferpool.get(blk);
	}
   
   private Buffer chooseUnpinnedBuffer() {
	   String buffer_policy= SimpleDB.BUFFER_REPLACEMENT_POLICY;
	   if(buffer_policy=="lru")
		   return unpinned.poll();
	   else if(buffer_policy=="mru")
		   return unpinned_mru.pop();
	   return null;
   }
   
   
   String listBuffer(){
		String s = "";
//		Iterable<Buffer> buffers = bufferpool.values();
		Collection<Buffer> list =bufferpool.values();
		
		for(int i=0;i<numBuffs;i++){
			Iterator<Buffer> it=list.iterator();
			while (it.hasNext()) {
				Buffer buff = it.next();
				if(buff.getBId()==i){
					s += buff.listBuffer();
					break;
				}
			}
		}
		return s;
   }
   
   synchronized boolean refreshment(){
	   if(unpinned.size()==bufferpool.size() || unpinned_mru.size()==bufferpool.size()){
		   bufferpool.clear();
		   unpinned.clear();
		   unpinned_mru.clear();
		   return true;
	   }
	   return false;
	   
   }
}
