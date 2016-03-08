package simpledb.multibuffer;

import static java.sql.Types.INTEGER;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.file.Block;
import simpledb.materialize.TempTable;
import simpledb.query.*;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * The class for the <i>chunk</i> operator.
 * 
 * @author Edward Sciore
 */
public class ChunkScan implements Scan {
	private List<RecordPage> pages;
	private int startbnum, endbnum, current;
	private Schema sch;
	private RecordPage rp;
	//utku
	private TempTable temp =null;  // used for chunk to tempfile. (split in ext-sort)

	/**
	 * Creates a chunk consisting of the specified pages.
	 * 
	 * @param ti
	 *            the metadata for the chunked table
	 * @param startbnum
	 *            the starting block number
	 * @param endbnum
	 *            the ending block number
	 * @param tx
	 *            the current transaction
	 */
	
	/**
	 * Bu chunkscan sadece input dosyasýnýn "bloklarýna" parça parça (chunk) eriþmek için kullanýlýyor. 
	 * O yüzden ti formatýndaki dosyanýn kendi sayfalarý.    
	 * Bu parçalarda herhangi bir deðiþiklik yapamayýz, yapmýyoruz, sadece okumak için. 
	 * @param ti
	 * @param startbnum
	 * @param endbnum
	 * @param tx
	 */
	public ChunkScan(TableInfo ti, int startbnum, int endbnum, Transaction tx) {
		pages = new ArrayList<RecordPage>();
		this.startbnum = startbnum;
		this.endbnum = endbnum;
		this.sch = ti.schema();
		String filename = ti.fileName();
		for (int i = startbnum; i <= endbnum; i++) {
			Block blk = new Block(filename, i);
			pages.add(new RecordPage(blk, ti, tx));
		}
		beforeFirst();
	}
	
	/**
	 * Bu chunkscan'da  size kadar bir alan ayýrýyoruz. Bu alan esasýnda sch formatýnda bir tempdosyasýnýn 
	 * bloklarý. Bu alan ilk olarak boþ. Bu alaný  insertFromScan ile dolduracagiz. 
	 * @param size
	 * @param sch
	 * @param tx
	 */
	public ChunkScan(int size, Schema sch, Transaction tx) {
		
		this.startbnum = 0;
		this.endbnum = size-1;
		this.sch = sch;
		pages = new ArrayList<RecordPage>();
		
		temp = new TempTable(sch, size, tx);
		TableInfo tiTemp = temp.getTableInfo();

		
		String filename = tiTemp.fileName();
		for (int i = startbnum; i <= endbnum; i++) {
			Block blk = new Block(filename, i);
			pages.add(new RecordPage(blk, tiTemp, tx));
		}
		beforeFirst();
	}
	/**
	 * @see simpledb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		moveToBlock(startbnum);
	}

	/**
	 * Moves to the next record in the current block of the chunk. If there are
	 * no more records, then make the next block be current. If there are no
	 * more blocks in the chunk, return false.
	 * 
	 * @see simpledb.query.Scan#next()
	 */
	public boolean next() {
		while (true) {
			if (rp.next())
				return true;
			if (current == endbnum)
				return false;
			moveToBlock(current + 1);
		}
	}

	/**
	 * @see simpledb.query.Scan#close()
	 */
	public void close() {
		for (RecordPage r : pages)
			r.close();
	}

	/**
	 * @see simpledb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		if (sch.type(fldname) == INTEGER)
			return new IntConstant(rp.getInt(fldname));
		else
			return new StringConstant(rp.getString(fldname));
	}

	/**
	 * @see simpledb.query.Scan#getInt(java.lang.String)
	 */
	public int getInt(String fldname) {
		return rp.getInt(fldname);
	}

	/**
	 * @see simpledb.query.Scan#getString(java.lang.String)
	 */
	public String getString(String fldname) {
		return rp.getString(fldname);
	}

	/**
	 * @see simpledb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		return sch.hasField(fldname);
	}

	private void moveToBlock(int blknum) {
		current = blknum;
		rp = pages.get(current - startbnum);
		rp.moveToId(-1);
	}
	
	
	// bundan sonrakiler benim yazdýklarým
	// Load chunk from source file.
	public boolean insertFromScan(Scan s) {
		if (!insert())
			return false;
		for (String fldname : sch.fields())
			setVal(fldname, s.getVal(fldname));
		return true;
	}
	
	// The chunk area is in fact a temptable. This function return the temp file. Used in split stage of sorting.
	public TempTable getAsTempTable(){
		close();
		return temp;
	}
//	public void copyToScan(UpdateScan s) {
//		s.insert();
//		for (String fldname : sch.fields())
//			s.setVal(fldname, getVal(fldname));
//	}
	
	// find an empty slot in the chunk. Always start searching from beginning. May be enhanced for big chunk area with additional search structures.
	public boolean insert(){
		beforeFirst();
		int curr=startbnum;
		while(!rp.insert()){
			if(curr+1>endbnum)
				return false;
			moveToBlock(++curr);
		}
		return true;
	}
	
	// performs a selection sort within a chunk
	public void internalSort(List<String> sortfields) {
		moveToBlock(startbnum);
		if (!next())
			return;
		int b1, s1, b2, s2; // b1,s1: ilk bloðun numarasý ve slotu; b2,s2:
							// ikinci bloðun numarasý ve slotu
		b1 = startbnum;
		s1 = rp.currentId();
		while (true) {
			int[] b2s2 = findSmallestFrom(b1, s1, sortfields);
			b2 = b2s2[0];
			s2 = b2s2[1];
			if (b1 != b2 || s1 != s2)
				swapRecords(b1, s1, b2, s2);
			moveToBlock(b1);
			rp.moveToId(s1);
			if (!next())
				return;
			b1 = current;
			s1 = rp.currentId();
		}
	}

	public int[] findSmallestFrom(int startBlock, int startId,
			List<String> sortfields) {

		moveToBlock(startBlock);
		rp.moveToId(startId);
		int minBlockId = startBlock;
		int minSlotId = startId;
		while (next()) {
			int nextBlockId = current;
			int nextSlotid = rp.currentId();
			if (compareRecords(minBlockId, minSlotId, nextBlockId, nextSlotid,
					sortfields) > 0) {
				minBlockId = nextBlockId;
				minSlotId = nextSlotid;
			}
			moveToBlock(nextBlockId);
			rp.moveToId(nextSlotid);
		}
		return new int[] { minBlockId, minSlotId };
	}

	private int compareRecords(int b1, int s1, int b2, int s2,
			List<String> sortfields) {
		for (String fldname : sortfields) {
			moveToBlock(b1);
			rp.moveToId(s1);
			Constant val1 = getVal(fldname);
			moveToBlock(b2);
			rp.moveToId(s2);
			Constant val2 = getVal(fldname);
			int result = val1.compareTo(val2);
			if (result != 0)
				return result;
		}
		return 0;
	}

	private void swapRecords(int b1, int s1, int b2, int s2) {
		for (String fldname : sch.fields()) {
			moveToBlock(b1);
			rp.moveToId(s1);
			Constant val1 = getVal(fldname);
			moveToBlock(b2);
			rp.moveToId(s2);
			Constant val2 = getVal(fldname);
			setVal(fldname, val1);
			moveToBlock(b1);
			rp.moveToId(s1);
			setVal(fldname, val2);
		}
	}

	public void setVal(String fldname, Constant val) {
		if (sch.type(fldname) == Types.INTEGER) {
			Integer ival = (Integer) val.asJavaVal();
			rp.setInt(fldname, ival.intValue());
		} 
		else if (sch.type(fldname) == Types.DOUBLE) {
			Double dval = (Double) val.asJavaVal();
			rp.setDouble(fldname, dval.doubleValue());
		} 
		else {
			String sval = (String) val.asJavaVal();
			rp.setString(fldname, sval);
		}
	}

	public void printChunk() {
		beforeFirst();
		int curpage = current;
		while (next()) {
			if (current != curpage)
				System.out.println("--------------");
			curpage = current;
			for (String fldname : sch.fields()) {
				System.out.print(getVal(fldname) + " ");
			}
			System.out.println();
		}
	}
	
	public RID getRid() {
		return new RID(current, rp.currentId());
	}

	public void moveToRid(RID rid) {
		moveToBlock(rid.blockNumber());
		rp.moveToId(rid.id());
	}
	
	public void delete(){
		rp.delete();
	}
	//utku
	@Override
	public double getDouble(String fldname) {
		return rp.getDouble(fldname);
	}
}