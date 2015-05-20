package simpledb.multibuffer;

import static simpledb.file.Page.*;
import static java.sql.Types.INTEGER;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.buffer.PageFormatter;
import simpledb.file.Block;
import simpledb.materialize.TempTable;
import simpledb.query.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The class for the <i>chunk</i> operator.
 * 
 * @author Edward Sciore
 */
public class WindowUpdateScan {
	private List<RecordPage> pages;
	private int startbnum, endbnum, current;
	private Schema sch;
	private RecordPage rp;

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
	public WindowUpdateScan(TableInfo ti, int startbnum, int endbnum,
			Transaction tx) {
		pages = new ArrayList<RecordPage>();
		this.startbnum = startbnum;
		this.endbnum = endbnum;
		this.sch = ti.schema();
		String filename = ti.fileName();
		
		// Window bölgesini Update edeceðimiz için slotlarý formatlamamýz gerekiyor.
		PageFormatter fmtr = new RecordFormatter(ti);
		for (int i = startbnum; i <= endbnum; i++) {
			tx.append(ti.fileName(), fmtr);
		}
		int size = tx.size(ti.fileName());  
		for (int i = startbnum; i <= endbnum; i++) {
			Block blk = new Block(filename, i);
			RecordPage rp=new RecordPage(blk, ti, tx);
			pages.add(rp);
			while(rp.next())
					System.out.print("");
		}
		
		beforeFirst();
	}

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

	protected void moveToBlock(int blknum) {
		current = blknum;
		rp = pages.get(current - startbnum);
		rp.moveToId(-1);
	}

	// @Override
	public void setVal(String fldname, Constant val) {
		// TODO Auto-generated method stub
		if (sch.type(fldname) == INTEGER)
			rp.setInt(fldname, (Integer) val.asJavaVal());
		else
			rp.setString(fldname, (String) val.asJavaVal());
	}

	public void setInt(String fldname, int val) {
		// TODO Auto-generated method stub
		rp.setInt(fldname, val);
	}

	public void setString(String fldname, String val) {
		// TODO Auto-generated method stub
		rp.setString(fldname, val);
	}

	/*
	 * INSERT edecek BOÞ bir slot arýyoruz. Fakat bulamayabiliriz. Çünkü Window geniþlemiyor.
	 */
	public boolean insert() {
		// TODO Auto-generated method stub
		for (int i = startbnum; i <= endbnum; i++) {
			moveToBlock(i);
			if (rp.insert())
				return true;
		}
		return false;
	}

	public void delete() {
		// TODO Auto-generated method stub
		rp.delete();
	}

	public RID getRid() {
		// TODO Auto-generated method stub
		return new RID(current, rp.currentId());
	}

	public void moveToRid(RID rid) {
		// TODO Auto-generated method stub
		moveToBlock(rid.blockNumber());
		rp.moveToId(rid.id());
	}

}