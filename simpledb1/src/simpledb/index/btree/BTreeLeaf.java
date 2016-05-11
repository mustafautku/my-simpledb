package simpledb.index.btree;

import simpledb.file.Block;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.Constant;

/**
 * An object that holds the contents of a B-tree leaf block.
 * @author Edward Sciore
 */
public class BTreeLeaf {
   private TableInfo ti;
   private Transaction tx;
   private Constant searchkey;
   private BTreePage contents;
   private int currentslot;
   
   /**
    * Opens a page to hold the specified leaf block.
    * The page is positioned immediately before the first record
    * having the specified search key (if any).
    * @param blk a reference to the disk block
    * @param ti the metadata of the B-tree leaf file
    * @param searchkey the search key value
    * @param tx the calling transaction
    */
   public BTreeLeaf(Block blk, TableInfo ti, Constant searchkey, Transaction tx) {
      this.ti = ti;
      this.tx = tx;
      this.searchkey = searchkey;
      contents = new BTreePage(blk, ti, tx);
      currentslot = contents.findSlotBefore(searchkey);
   }
   
   /**
    * Closes the leaf page.
    */
   public void close() {
      contents.close();
   }
   
   /**
    * Moves to the next leaf record having the 
    * previously-specified search key.
    * Returns false if there is no more such records.
    * @return false if there are no more leaf records for the search key
    */
//   public boolean next() {
   public boolean next(Statistics m_stats) {
      currentslot++;
      if (currentslot >= contents.getNumRecs()) 
         return tryOverflow(m_stats);
      else if (contents.getDataVal(currentslot).equals(searchkey))
         return true;
      else 
         return tryOverflow(m_stats);
   }
   
   // utku: Another "special" next func. for range query exec. 
   /*
    * It is difficult to return back to main leaf node after processing the OFs. 
    * Thus, first we first process all values in the leaf, then OFs are executed. This naturally changes the order.
    */
   public boolean nextLessThan(Constant rangeEnd,Statistics m_stats){
	   currentslot++;
	   if (currentslot >= contents.getNumRecs()) 
		   return tryOverflow(m_stats);//return false; 
	   
	   Constant c=contents.getDataVal(currentslot);
	   if(currentslot==0) searchkey=c;
	   if (c.compareTo(rangeEnd)<=0)
	         return true;
	   else 
	         return tryOverflow(m_stats);

   }
   
   /**
    * Returns the dataRID value of the current leaf record.
    * @return the dataRID of the current record
    */
   public RID getDataRid() {
      return contents.getDataRid(currentslot);
   }
   
   /**
    * Deletes the leaf record having the specified dataRID
    * @param datarid the dataRId whose record is to be deleted
    */
   public void delete(RID datarid,Statistics m_stats) {
      while(next(m_stats))
         if(getDataRid().equals(datarid)) {
         contents.delete(currentslot);
         return;
      }
   }
   
   /**
    * Inserts a new leaf record having the specified dataRID
    * and the previously-specified search key.
    * If the record does not fit in the page, then 
    * the page splits and the method returns the
    * directory entry for the new page;
    * otherwise, the method returns null.  
    * If all of the records in the page have the same dataval,
    * then the block does not split; instead, all but one of the
    * records are placed into an overflow block.
    * @param datarid the dataRID value of the new record
    * @return the directory entry of the newly-split page, if one exists.
    */
   public DirEntry insert(RID datarid,Statistics m_stats) {
   	// bug fix:  If the page has an overflow page 
   	// and the searchkey of the new record would be lowest in its page, 
   	// we need to first move the entire contents of that page to a new block
   	// and then insert the new record in the now-empty current page.
   	if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchkey) > 0) {
   		Constant firstval = contents.getDataVal(0);
   		Block newblk = contents.split(0, contents.getFlag());
   		currentslot = 0;
   		contents.setFlag(-1);
   		contents.insertLeaf(currentslot, searchkey, datarid); 
   		m_stats.m_leafs++;   	
   		m_stats.m_splits++;
   		return new DirEntry(firstval, newblk.number());  
   	}
	  
      currentslot++;
      contents.insertLeaf(currentslot, searchkey, datarid);
      if (!contents.isFull())
         return null;
      // else page is full, so split it
      Constant firstkey = contents.getDataVal(0);
      Constant lastkey  = contents.getDataVal(contents.getNumRecs()-1);
      if (lastkey.equals(firstkey)) {
         // create an overflow block to hold all but the first record
         Block newblk = contents.split(1, contents.getFlag());
         contents.setFlag(newblk.number());
         m_stats.m_OFnodes++;
         m_stats.m_splits++;
         return null;
      }
      else {
         int splitpos = contents.getNumRecs() / 2;
         Constant splitkey = contents.getDataVal(splitpos);
         if (splitkey.equals(firstkey)) {
            // move right, looking for the next key
            while (contents.getDataVal(splitpos).equals(splitkey))
               splitpos++;
            splitkey = contents.getDataVal(splitpos);
         }
         else {
            // move left, looking for first entry having that key
            while (contents.getDataVal(splitpos-1).equals(splitkey))
               splitpos--;
         }
         Block newblk = contents.split(splitpos, -1);
         m_stats.m_leafs++;
         m_stats.m_splits++;
         return new DirEntry(splitkey, newblk.number());
      }
   }
   
//   private boolean tryOverflow() {
   private boolean tryOverflow(Statistics m_stats) {
      Constant firstkey = contents.getDataVal(0);
      int flag = contents.getFlag();
      if (!searchkey.equals(firstkey) || flag < 0)
         return false;
      contents.close();
      Block nextblk = new Block(ti.fileName(), flag);
      contents = new BTreePage(nextblk, ti, tx);
      m_stats.m_reads++;
      currentslot = 0;
      return true;
   }
   
   
}
