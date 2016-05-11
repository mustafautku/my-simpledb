package simpledb.index.btree;

import java.util.ArrayList;

/*
 * example of a Strategy pattern: traverses the tree by LEVEL. BREATH-FIRST TRAVERSAL.
 * Used for calculating tree characteristics s.a. heigth, #of nodes, nodes in each level...
 */

class MyQueryStrategy1 implements IQueryStrategy {
	private ArrayList idxids = new ArrayList();
	private ArrayList leafids = new ArrayList();
	private ArrayList OFids= new ArrayList();
	private boolean reachedLeaf=false;

	public void getNextEntry(BTreePage page, int[] nextEntry, boolean[] hasNext) {
		int level = page.getFlag();  // level>=0 means more blocks are added. Leaf has OF block. Dir has lower level IDX blocks
		
		// traverse "index nodes at levels 0 and higher" + "leaf nodes" 
		if (!reachedLeaf && level > 0) {
			for (int cChild = 0; cChild < page.getNumRecs(); cChild++) {
				idxids.add(new Integer(page.getChildNum(cChild)));
			}
		}
		else if(level==0){
			reachedLeaf=true;
			for (int cChild = 0; cChild < page.getNumRecs(); cChild++) {
				leafids.add(new Integer(page.getChildNum(cChild)));
			}
		}
		else if(reachedLeaf && level > 0){
			OFids.add(new Integer(level));
		}

		if (!idxids.isEmpty()) {
			nextEntry[0] = ((Integer) idxids.remove(0)).intValue();
			hasNext[0] = true;
			hasNext[1]=false;
		} 
		else if(!leafids.isEmpty()){
			nextEntry[0] = ((Integer) leafids.remove(0)).intValue();
			hasNext[0] = false;
			hasNext[1]=true;
		}
		else if(!OFids.isEmpty()){
			nextEntry[0] = ((Integer) OFids.remove(0)).intValue();
			hasNext[0] = false;
			hasNext[1]=false;
			hasNext[2]=true;
		}
		else {
			hasNext[0] = false;
			hasNext[1]=false;
			hasNext[2]=false;
		}
		page.close();
	}
};


//class MyQueryStrategy implements IQueryStrategy {
//	private ArrayList ids = new ArrayList();
//
//	public void getNextEntry(BTreePage n, int[] nextEntry, boolean[] hasNext) {
//		int level = n.getFlag();  // level>=0 means more blocks are added. Leaf has OF block. Dir has lower level IDX blocks
//		
//		// traverse "index nodes at levels 0 and higher" + "lefa nodes" + "OF nodes"
//		if (level >= 0) {
//			for (int cChild = 0; cChild < n.getNumRecs(); cChild++) {
//				ids.add(new Integer(n.getChildNum(cChild)));
//			}
//		}
//
//		if (!ids.isEmpty()) {
//			nextEntry[0] = ((Integer) ids.remove(0)).intValue();
//			hasNext[0] = true;
//		} else {
//			hasNext[0] = false;
//		}
//	}
//};
