package simpledb.index.btree;

import static java.sql.Types.INTEGER;

import java.util.ArrayList;
import java.util.Stack;

import simpledb.file.Block;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;



/**
 * A B-tree implementation of the Index interface.
 * @author Edward Sciore
 */
public class BTreeIndex implements Index {
   private Transaction tx;
   private TableInfo dirTi, leafTi;
   private BTreeLeaf leaf = null;
   private Block rootblk;
   private Statistics m_stats;
   // below is for range query
   private ArrayList rangeLeafs=new ArrayList(); 
   private Constant[] queryRange;
   private boolean moreExistInPage=true;
//   private MyVisitor vis=new MyVisitor();

   /**
    * Opens a B-tree index for the specified index.
    * The method determines the appropriate files
    * for the leaf and directory records,
    * creating them if they did not exist.
    * @param idxname the name of the index
    * @param leafsch the schema of the leaf index records
    * @param tx the calling transaction
    */
   public BTreeIndex(String idxname, Schema leafsch, Transaction tx) {
      this.tx = tx;
      m_stats = new Statistics();
      // deal with the leaves
      String leaftbl = idxname + "leaf";
      leafTi = new TableInfo(leaftbl, leafsch);
      if (tx.size(leafTi.fileName()) == 0){
         tx.append(leafTi.fileName(), new BTPageFormatter(leafTi, -1));
         m_stats.m_leafs++;
      }
      
      // deal with the directory
      Schema dirsch = new Schema();
      dirsch.add("block",   leafsch);
      dirsch.add("dataval", leafsch);
      String dirtbl = idxname + "dir";
      dirTi = new TableInfo(dirtbl, dirsch);
      rootblk = new Block(dirTi.fileName(), 0);
      if (tx.size(dirTi.fileName()) == 0){
         // create new root block
         tx.append(dirTi.fileName(), new BTPageFormatter(dirTi, 0));
         m_stats.m_nodes++;
         m_stats.m_treeHeight=1;
         m_stats.m_nodesInLevel.add(new Integer(1));
      }
      BTreePage page = new BTreePage(rootblk, dirTi, tx);
      if (page.getNumRecs() == 0) {
			// insert initial directory entry
         int fldtype = dirsch.type("dataval");
         Constant minval = (fldtype == INTEGER) ?
            new IntConstant(Integer.MIN_VALUE) :
            new StringConstant("");
         page.insertDir(0, minval, 0);
		}
      page.close();
   }

   /**
    * Traverses the directory to find the leaf block corresponding
    * to the specified search key.
    * The method then opens a page for that leaf block, and
    * positions the page before the first record (if any)
    * having that search key.
    * The leaf page is kept open, for use by the methods next
    * and getDataRid.
    * @see simpledb.index.Index#beforeFirst(simpledb.query.Constant)
    */
   public void beforeFirst(Constant searchkey) {
      close();
      BTreeDir root = new BTreeDir(rootblk, dirTi, tx);
      int blknum = root.search(searchkey,m_stats);
      root.close();
      Block leafblk = new Block(leafTi.fileName(), blknum);
      leaf = new BTreeLeaf(leafblk, leafTi, searchkey, tx);
      m_stats.m_reads++;  // for the first leaf..Others may be OF leafs..
      queryRange=null;
   }

   /**
    * Moves to the next leaf record having the
    * previously-specified search key.
    * Returns false if there are no more such leaf records.
    * @see simpledb.index.Index#next()
    */
	public boolean next() {
		if (queryRange==null){
			boolean hasNext=leaf.next(m_stats);
			if(hasNext) m_stats.m_queryResults++;
			return hasNext;
		}
		if(moreExistInPage){
			moreExistInPage=leaf.nextLessThan(queryRange[1],m_stats);
			if(!moreExistInPage && !rangeLeafs.isEmpty()){
				leaf.close();
				Block leafblk = new Block(leafTi.fileName(), (Integer)rangeLeafs.remove(0));
				leaf = new BTreeLeaf(leafblk, leafTi,new IntConstant(Integer.MIN_VALUE) , tx);
				m_stats.m_reads++;
				moreExistInPage=leaf.nextLessThan(queryRange[1],m_stats);
			}
//			return moreExistInPage;
			if(moreExistInPage) m_stats.m_queryResults++;
			return moreExistInPage;
		}	
		return false;
	}

   /**
    * Returns the dataRID value from the current leaf record.
    * @see simpledb.index.Index#getDataRid()
    */
   public RID getDataRid() {
      return leaf.getDataRid();
   }

   /**
    * Inserts the specified record into the index.
    * The method first traverses the directory to find
    * the appropriate leaf page; then it inserts
    * the record into the leaf.
    * If the insertion causes the leaf to split, then
    * the method calls insert on the root,
    * passing it the directory entry of the new leaf page.
    * If the root node splits, then makeNewRoot is called.
    * @see simpledb.index.Index#insert(simpledb.query.Constant, simpledb.record.RID)
    */
   public void insert(Constant dataval, RID datarid) {
	   m_stats.m_data++;

      beforeFirst(dataval);
      DirEntry e = leaf.insert(datarid, m_stats);
      leaf.close();
      if (e == null)
         return;
      
      BTreeDir root = new BTreeDir(rootblk, dirTi, tx);
      DirEntry e2 = root.insert(e,m_stats); // here we have 2 blocks: 0 and 1. We need a root.
      if (e2 != null){
         root.makeNewRoot(e2,m_stats); // always keep root at block-0
      }
      root.close();
   }

   /**
    * Deletes the specified index record.
    * The method first traverses the directory to find
    * the leaf page containing that record; then it
    * deletes the record from the page.
    * @see simpledb.index.Index#delete(simpledb.query.Constant, simpledb.record.RID)
    */
   public void delete(Constant dataval, RID datarid) {
      beforeFirst(dataval);
      leaf.delete(datarid,m_stats);
      leaf.close();
      m_stats.m_data--;
   }

   /**
    * Closes the index by closing its open leaf page,
    * if necessary.
    * @see simpledb.index.Index#close()
    */
   public void close() {
      if (leaf != null)
         leaf.close();
   }

   /**
    * Estimates the number of block accesses
    * required to find all index records having
    * a particular search key.
    * @param numblocks the number of blocks in the B-tree directory
    * @param rpb the number of index entries per block
    * @return the estimated traversal cost
    */
   public static int searchCost(int numblocks, int rpb) {
      return 1 + (int)(Math.log(numblocks) / Math.log(rpb));
   }
   
   
   // utku:
//   public IStatistics getStatistics()
//	{
//		return (IStatistics) m_stats.clone();
//	}
   public Statistics getStatistics()
 	{
 		return  (Statistics) m_stats.clone();
 	}
   
	public IStatistics calculateStatistics() { // breath-first traversal
	
		MyQueryStrategy1 qs = new MyQueryStrategy1();
		int[] next = new int[] { 0 };  //root'dan basliyoruz.
		Block blk = new Block(dirTi.fileName(), next[0]);
		BTreePage page = new BTreePage(blk, dirTi, tx);
		m_stats.m_nodes++;
		int level=page.getFlag();
		for(int i=0;i<level;i++)
			m_stats.m_nodesInLevel.add(new Integer(0));
		m_stats.m_nodesInLevel.add(level,new Integer(1));
		m_stats.m_treeHeight=level+1;
		
		while (true) {
			boolean[] hasNext = new boolean[] { false, false, false};
			qs.getNextEntry(page, next, hasNext);

			if ((hasNext[0] == true)) { // DIR pages.
				blk = new Block(dirTi.fileName(), next[0]);
				page = new BTreePage(blk, dirTi, tx);
				m_stats.m_nodes++;
				level = page.getFlag();
				int i = ((Integer) m_stats.m_nodesInLevel.get(level))
						.intValue();
				m_stats.m_nodesInLevel.set(level, new Integer(i + 1));
			} else if ((hasNext[1] == true)) { // LEAF pages.
				blk = new Block(leafTi.fileName(), next[0]);
				page = new BTreePage(blk, leafTi, tx);
				m_stats.m_leafs++;
				m_stats.m_data += page.getNumRecs();
			} else if ((hasNext[2] == true)) { // OF pages.
				blk = new Block(leafTi.fileName(), next[0]);
				page = new BTreePage(blk, leafTi, tx);
				m_stats.m_OFnodes++;
				m_stats.m_data += page.getNumRecs();
			} 
			else if (hasNext[0] == false && hasNext[1] == false) // DONE
				break;
			else
				System.err.println("errorrr");
		}
		
		return (IStatistics) m_stats.clone();
	}
		
	/// This function MAY (MUST) be added into INDEX interface..	
	public void beforeFirst(Constant[] range) {
		close();
		queryRange = range.clone();
		BTreeDir root = new BTreeDir(rootblk, dirTi, tx);
		m_stats.m_reads++;
		root.searchForRangeQuery(range, rangeLeafs,m_stats);
		root.close();
		Block leafblk = new Block(leafTi.fileName(),
				(Integer) rangeLeafs.remove(0));
		leaf = new BTreeLeaf(leafblk, leafTi, queryRange[0], tx);
		m_stats.m_reads++;  // for the first leaf..Others may be OF of following leafs..
	}
	public void resetQueryStats(){
		m_stats.m_reads=0;
		m_stats.m_queryResults=0;
	}
}
