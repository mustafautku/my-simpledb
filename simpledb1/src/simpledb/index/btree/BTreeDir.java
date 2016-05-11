package simpledb.index.btree;

import java.util.ArrayList;
import java.util.Stack;

import simpledb.file.Block;
import simpledb.tx.Transaction;
import simpledb.record.TableInfo;
import simpledb.query.Constant;


/**
 * A B-tree directory block.
 * @author Edward Sciore
 */
public class BTreeDir {
   private TableInfo ti;
   private Transaction tx;
   private String filename;
   private BTreePage contents;

   /**
    * Creates an object to hold the contents of the specified
    * B-tree block.
    * @param blk a reference to the specified B-tree block
    * @param ti the metadata of the B-tree directory file
    * @param tx the calling transaction
    */
   BTreeDir(Block blk, TableInfo ti, Transaction tx) {
      this.ti = ti;
      this.tx = tx;
      filename = blk.fileName();
      contents = new BTreePage(blk, ti, tx);
   }

   /**
    * Closes the directory page.
    */
   public void close() {
      contents.close();
   }

   /**
    * Returns the block number of the B-tree leaf block
    * that contains the specified search key.
    * @param searchkey the search key value
    * @return the block number of the leaf block containing that search key
    */
   public int search(Constant searchkey, Statistics m_stats) {
      Block childblk = findChildBlock(searchkey);
      m_stats.m_reads++;
      while (contents.getFlag() > 0) {
         contents.close();
         contents = new BTreePage(childblk, ti, tx);
         m_stats.m_reads++;
         childblk = findChildBlock(searchkey);
      }
      return childblk.number();
   }

   /**
    * Creates a new root block for the B-tree.
    * The new root will have two children:
    * the old root, and the specified block.
    * Since the root must always be in block 0 of the file,
    * the contents of the old root will get transferred to a new block.
    * @param e the directory entry to be added as a child of the new root
    */
   public void makeNewRoot(DirEntry e,Statistics m_stats) {
      Constant firstval = contents.getDataVal(0);
      int level = contents.getFlag();
      Block newblk = contents.split(0, level); //ie, transfer all the records
      DirEntry oldroot = new DirEntry(firstval, newblk.number());
      insertEntry(oldroot,m_stats);
      insertEntry(e,m_stats);
      m_stats.m_nodes++;
      contents.setFlag(level+1);
      m_stats.m_treeHeight++;
      m_stats.m_nodesInLevel.add(new Integer(1));
   }

   /**
    * Inserts a new directory entry into the B-tree block.
    * If the block is at level 0, then the entry is inserted there.
    * Otherwise, the entry is inserted into the appropriate
    * child node, and the return value is examined.
    * A non-null return value indicates that the child node
    * split, and so the returned entry is inserted into
    * this block.
    * If this block splits, then the method similarly returns
    * the entry information of the new block to its caller;
    * otherwise, the method returns null.
    * @param e the directory entry to be inserted
    * @return the directory entry of the newly-split block, if one exists; otherwise, null
    */
   public DirEntry insert(DirEntry e,Statistics m_stats) {
      if (contents.getFlag() == 0)
         return insertEntry(e,m_stats);
      Block childblk = findChildBlock(e.dataVal());
      BTreeDir child = new BTreeDir(childblk, ti, tx);
      DirEntry myentry = child.insert(e,m_stats);
      child.close();
      return (myentry != null) ? insertEntry(myentry,m_stats) : null;
   }

   private DirEntry insertEntry(DirEntry e,Statistics m_stats) {
      int newslot = 1 + contents.findSlotBefore(e.dataVal());
      contents.insertDir(newslot, e.dataVal(), e.blockNumber());
      if (!contents.isFull())
         return null;
      // else page is full, so split it
      int level = contents.getFlag();
      int splitpos = contents.getNumRecs() / 2;
      Constant splitval = contents.getDataVal(splitpos);
      Block newblk = contents.split(splitpos, level);
      m_stats.m_nodes++;
      m_stats.m_splits++;
      int i = ((Integer) m_stats.m_nodesInLevel.get(level)).intValue();
	  m_stats.m_nodesInLevel.set(level, new Integer(i + 1));
      return new DirEntry(splitval, newblk.number());
   }

   private Block findChildBlock(Constant searchkey) {
      int slot = contents.findSlotBefore(searchkey);
      if (contents.getDataVal(slot+1).equals(searchkey))
         slot++;
      int blknum = contents.getChildNum(slot);
      return new Block(filename, blknum);
   }
     
   //utku
   // find the highest slotId with value <searchkey
   private int findChildBlockSlot(Constant searchkey) {
	      int slot = contents.findSlotBefore(searchkey);
	      if (contents.getDataVal(slot+1).equals(searchkey))
	         slot++;
	      return slot;
	   }
/*
 * Traverses over the search area of the DirPages only. (meanwhile collect the leaf pages w/o reading them yet)
 */
   public boolean searchForRangeQuery(Constant[] range, ArrayList rangeLeafIds, Statistics m_stats){
	   Stack st=new Stack();
	   st.push(contents.getBlkId());
	   
		while (contents.getFlag() > 0) { // level-0 dir will be stacked. Leafs
											// won't.
			Block childBlk = findChildBlock(range[0]);
			st.push(childBlk.number());
			contents.close();  // always ONLY 1 contents is open. Otherwise bufferpool overload.	
			contents = new BTreePage(childBlk, ti, tx);	
			m_stats.m_reads++;
		}
		if (contents.getFlag() == 0) {
			int MLslot = findChildBlockSlot(range[0]);  // this slot has the MAX value < rangeMin
			int MRslot = findChildBlockSlot(range[1]);
			MLslot = (MLslot==-1)?0:MLslot;
			for (int i = MLslot; i <= MRslot; i++)
				rangeLeafIds.add(contents.getChildNum(i));
			if (MRslot < contents.getNumRecs()-1){
				contents.close();
				return false;
			}
		}
		contents.close();
		int lastProcessedBlockId = (Integer) st.pop();
		while (!st.empty()) {  // If no more node in stack, then 
			Block currentBlk = new Block(ti.fileName(), (Integer) st.peek());
			contents = new BTreePage(currentBlk, ti, tx);
			m_stats.m_reads++;
			int slot = 0;
			for (; slot < contents.getNumRecs(); slot++) {
				if (contents.getChildNum(slot) == lastProcessedBlockId)
					break;
			}
			if (slot == contents.getNumRecs()-1) {
				lastProcessedBlockId = currentBlk.number();
				contents.close();
				st.pop();
				continue;
			}
			int nextBlkId = contents.getChildNum(slot + 1);
			Block newSubRootBlock = new Block(ti.fileName(), nextBlkId);
			contents.close(); // always ONLY 1 contents is open. Otherwise bufferpool overload.
			contents = new BTreePage(newSubRootBlock, ti, tx);
			m_stats.m_reads++;
			range[0]= contents.getDataVal(0);
			
			boolean isMore = this.searchForRangeQuery(range, rangeLeafIds,m_stats);
			
			if (!isMore){
				break; //return false;
			}
			lastProcessedBlockId = newSubRootBlock.number();
		}
		return true;
	}
}
