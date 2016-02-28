package simpledb.multibuffer;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;

import java.util.ArrayList;
import java.util.List;

import simpledb.file.Block;
import simpledb.materialize.TempTable;
import simpledb.query.Constant;
import simpledb.query.IntConstant;
import simpledb.query.Scan;
import simpledb.query.StringConstant;
import simpledb.query.UpdateScan;
import simpledb.record.RID;
import simpledb.record.RecordPage;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

/**
 * HEap organization within a buffer pool. Used with replacement selection paradigm.
 * @author mustafautku
 *
 */
public class ChunkHeap {
	private List<String> sf;
	private int maxheapsize;
	private int currentheapsize;
	private int RPB;
	private List<RecordPage> pages;
	private int startbnum, endbnum, current; // current page number
	private Schema sch;
	private RecordPage rp; // current page

	public ChunkHeap(TableInfo ti, int startbnum, int endbnum,
			List<String> sortfields, Transaction tx) {
		pages = new ArrayList<RecordPage>();
		this.startbnum = startbnum;
		this.endbnum = endbnum;
		this.sch = ti.schema();
		TempTable stageArea = new TempTable(ti.schema(), tx); // this temp file
																// represents
																// the stage
																// area. Will
																// not extend!!
		for (int i = startbnum; i <= endbnum; i++) {
			Block blk = new Block(stageArea.getTableInfo().fileName(), i);
			pages.add(new RecordPage(blk, ti, tx));
		}
		sf = sortfields;
		int slotsize = ti.recordLength() + INT_SIZE;
		RPB = (int) BLOCK_SIZE / slotsize;
		maxheapsize = RPB * (endbnum - startbnum + 1); 
		currentheapsize = 0;
	}

	public void close() {
		for (RecordPage r : pages)
			r.close();
	}

	public Constant getVal(String fldname) {
		if (sch.type(fldname) == INTEGER)
			return new IntConstant(rp.getInt(fldname));
		else
			return new StringConstant(rp.getString(fldname));
	}

	public int getInt(String fldname) {
		return rp.getInt(fldname);
	}

	public String getString(String fldname) {
		return rp.getString(fldname);
	}

	public boolean hasField(String fldname) {
		return sch.hasField(fldname);
	}

	public void setVal(String fldname, Constant val) {
		if (sch.type(fldname) == INTEGER)
			rp.setInt(fldname, (Integer) val.asJavaVal());
		else
			rp.setString(fldname, (String) val.asJavaVal());
	}

	public void setInt(String fldname, int val) {
		rp.setInt(fldname, val);
	}

	public void setString(String fldname, String val) {
		rp.setString(fldname, val);
	}
	
	public boolean loadHeap(Scan iscan) { 	
		if(currentheapsize !=0)
			System.exit(-1);
		boolean moreRecords=true;
		if (iscan != null) {
			while (currentheapsize < maxheapsize) {
				RID nrid = new RID((int) currentheapsize / RPB, currentheapsize % RPB);
				moveToRID(nrid);
				for (String fldname : sch.fields())
					setVal(fldname, iscan.getVal(fldname));
				currentheapsize++;
				if (!iscan.next()) {
					moreRecords=false;
					break;
				}
			}
			if(!moreRecords)  // if stage is not exact full, then reset maxheapsize as currentheapsize
				maxheapsize=currentheapsize;
		}
		currentheapsize=0;
		for(int i=0;i<maxheapsize;i++){
			RID nrid = new RID((int) currentheapsize / RPB, currentheapsize % RPB);
			moveToRID(nrid);
			int n = currentheapsize;
			while (n > 0) { // until we reach the root of the heap
				int p = (n - 1) / 2; // the index of the parent of n
				RID prid = new RID((int) p / RPB, p % RPB);
				if (compareRecords(nrid, prid, sf) < 0) {
					swapRecords(nrid, prid); // swap child with parent
					n = p; // check parent
					nrid = prid;
				} else // parent is smaller than child					
					break; // all is good in the heap
			}
			currentheapsize++;
		}
		return moreRecords;
	}

	public boolean insertIntoHeap(Scan s) {
		if(currentheapsize == 0)   // end of a pass. 
			return false;		
		
		RID nrid = new RID(0, 0);
		moveToRID(nrid);
		int compres = compareRecords(s, nrid, sf);
		if (compres > 0) {  // new record is larger. Place it on the root. (Root has been sent already.) Then heapify if new>root. 
			for (String fldname : sch.fields())
				setVal(fldname, s.getVal(fldname));
			if(currentheapsize == 1) 
				return true;
			heapify();
			return true;
		} else if (compres == 0) // no need to heapify.
			return true;		
	
		// we are here. Thus new record unfortunately is smaller. We'll sent it to the end of staged area. 
		//First, place the last record of the current heap into root (heapify it)
		RID endrid = new RID((int) (currentheapsize-1) / RPB, (currentheapsize-1) % RPB);
		swapRecords(nrid, endrid);
		moveToRID(endrid);
		for (String fldname : sch.fields())
			setVal(fldname, s.getVal(fldname));
		--currentheapsize;
		if (currentheapsize > 1) 
			heapify();
		return true;
	}

	public boolean flushHeap(UpdateScan s1, boolean last) {
		int i;
//		int counter=0;
//		int k;
		// for debug, print.
//		for(i=0;i<=endbnum;i++){
//			for(k=0;k<RPB;k++){
//				RID temprid=new RID(i,k);
//				moveToRID(temprid);
//				for (String fldname : sf) 
//					System.out.print(" " + rp.getInt(fldname));
//				counter++;
//				if(counter==currentheapsize)
//					System.out.print(" <----> ");				
//			}
//			System.out.println(" | ");
//		}
		
		// We have a heap at the beginning of the staged area. Transfer it to output file:
		int startIdxofLastHeap=currentheapsize;
		RID nrid = new RID(0, 0);
		RID endrid = new RID((int) (currentheapsize-1) / RPB, (currentheapsize-1) % RPB);
		if(last){
			moveToRID(nrid);
			s1.insert();
			for (String fldname : sch.fields())
				s1.setVal(fldname, getVal(fldname));
		}
			
		while(currentheapsize > 1){
			swapRecords(nrid, endrid); // root has already been sent before.
			currentheapsize--;
			heapify();
			moveToRID(nrid);
			s1.insert();
			for (String fldname : sch.fields())
				s1.setVal(fldname, getVal(fldname));
			endrid = new RID((int) (currentheapsize-1) / RPB, (currentheapsize-1) % RPB);
		}
		if(last)
			return false;
		
		
		// now the last records outside of the heap. Start a new (last) heap
		currentheapsize=0; // initiate e new heap (last one)
		RID irid,torid;
		for(i=startIdxofLastHeap;i<maxheapsize;i++){
			irid = new RID((int) i / RPB, i % RPB);
			torid=new RID((int) currentheapsize / RPB, currentheapsize % RPB);
			copyToSlot(irid,torid);
			
			nrid = new RID((int) currentheapsize / RPB, currentheapsize % RPB);
			
			int n = currentheapsize;

			while (n > 0) { // until we reach the root of the heap
				int p = (n - 1) / 2; // the index of the parent of n
				RID prid = new RID((int) p / RPB, p % RPB);
				if (compareRecords(nrid, prid, sf) < 0) {
					swapRecords(nrid, prid); // swap child with parent
					n = p; // check parent
					nrid = prid;
				} else// parent is smaller than child					
					break; // all is good in the heap
			}
			currentheapsize++;
		}
		if(currentheapsize>0) return true;
		return false;
	}
	
	public boolean copyToOutputScan(UpdateScan s) {
		if (currentheapsize == 0)
			return false;
		moveToRID(new RID(0,0));
		s.insert();
		for (String fldname : sch.fields())
			s.setVal(fldname, getVal(fldname));
		return true;
	}
	
	
	private void heapify() {
		RID nrid = new RID(0, 0);
		moveToRID(nrid);
		int n = 0;
		while (true) {
			int left = (n * 2) + 1;
			if (left >= currentheapsize) // node has no left child
				break; // reached the bottom; heap is heapified
			int right = left + 1;
			RID lrid = new RID((int) left / RPB, left % RPB);
			RID rrid = new RID((int) right / RPB, right % RPB);
			if (right >= currentheapsize) { // node has a left child, but no right child				
				if (compareRecords(lrid, nrid, sf) < 0)//  if left child is smaller than // node
					swapRecords(lrid, nrid); // swap left child with node
				break; // heap is heapified
			}			 
			if (compareRecords(lrid, nrid, sf) < 0) {// (left < n)
				if (compareRecords(lrid, rrid, sf) < 0) {
					swapRecords(lrid, nrid);
					n = left;
					nrid = lrid;
					continue; // continue recursion on left child
				} else { // (right < left < n)
					swapRecords(rrid, nrid);
					n = right;
					nrid = rrid;
					continue; // continue recursion on right child
				}
			} else { // (n < left)
				if (compareRecords(rrid, nrid, sf) < 0) { // (right < n < left)
					swapRecords(rrid, nrid);
					n = right;
					nrid = rrid;
					continue; // continue recursion on right child
				} else { // (n > left) & (n > right)
					break; // node is greater than both children, so it's heapified
				}
			}
		}
	}
	
	private void copyToSlot(RID r1, RID r2) {
		for (String fldname : sch.fields()) {
			moveToRID(r1);
			Constant val1 = getVal(fldname);
			moveToRID(r2);
			setVal(fldname, val1);
		}
	}	

	private void moveToRID(RID _rid){
		current = _rid.blockNumber();
		rp = pages.get(current - startbnum);
		rp.moveToId(_rid.id());
	}	
	
	private void swapRecords(RID r1, RID r2) {
		for (String fldname : sch.fields()) {
			moveToRID(r1);
			Constant val1 = getVal(fldname);
			moveToRID(r2);
			Constant val2 = getVal(fldname);
			setVal(fldname, val1);
			moveToRID(r1);
			setVal(fldname, val2);
		}
	}

	private int compareRecords(RID r1, RID r2, List<String> sortfields) {
		for (String fldname : sortfields) {
			moveToRID(r1);
			Constant val1 = getVal(fldname);
			moveToRID(r2);
			Constant val2 = getVal(fldname);
			int result = val1.compareTo(val2);
			if (result != 0)
				return result;
		}
		return 0;
	}

	private int compareRecords(Scan s, RID r2, List<String> sortfields) {

		for (String fldname : sortfields) {
			Constant val1 = s.getVal(fldname);
			moveToRID(r2);
			Constant val2 = getVal(fldname);
			int result = val1.compareTo(val2);
			if (result != 0)
				return result;
		}
		return 0;
	}
}
