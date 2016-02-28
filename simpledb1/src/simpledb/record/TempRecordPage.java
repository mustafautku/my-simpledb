package simpledb.record;

import java.sql.Types;
import java.util.List;

import simpledb.file.Block;
import simpledb.query.Constant;
import simpledb.query.IntConstant;
import simpledb.query.Scan;
import simpledb.query.StringConstant;
import simpledb.query.UpdateScan;
import simpledb.tx.Transaction;


/**
 * This class is basically used to manipulate "only" a buffer. Get/set functions and sorting within a page.
 * ChunkScan is more general, K=1,2,3,.... pages of chunk area to manipulate records. (sort, update,..)
 * 
 * @author mustafautku
 *
 */
public class TempRecordPage extends RecordPage {
	private Schema sch;

	public TempRecordPage(TableInfo ti, int blknum, Transaction tx) {
		super(new Block(ti.fileName(), blknum), ti, tx);
		sch = ti.schema();
	}

	public void setVal(String fldname, Constant val) {
		if (sch.type(fldname) == Types.INTEGER) {
			Integer ival = (Integer) val.asJavaVal();
			super.setInt(fldname, ival.intValue());
		} else {
			String sval = (String) val.asJavaVal();
			super.setString(fldname, sval);
		}
	}

	public Constant getVal(String fldname) {
		if (sch.type(fldname) == Types.INTEGER) {
			int ival = super.getInt(fldname);
			return new IntConstant(ival);
		} else {
			String sval = super.getString(fldname);
			return new StringConstant(sval);
		}
	}

	public int findSmallestFrom(int startId, List<String> sortfields) {
		int minId = startId;
		super.moveToId(startId);
		while (super.next()) {
			int id = super.currentId();
			if (minId < 0 || compareRecords(minId, id, sortfields) > 0)
				minId = id;
			super.moveToId(id);
		}
		return minId;
	}

	public int findSmallestAsBigAs(Scan s, List<String> sortfields) {
		int minId = -1;
		super.moveToId(-1);
		while (super.next()) {
			int id = super.currentId();
			if (compareRecords(id, s, sortfields) >= 0) {
				if (minId < 0 || compareRecords(minId, id, sortfields) > 0)
					minId = id;
			}
			super.moveToId(id);
		}
		return minId;
	}

	public void swapRecords(int id1, int id2) {
		for (String fldname : sch.fields()) {
			super.moveToId(id1);
			Constant val1 = getVal(fldname);
			super.moveToId(id2);
			Constant val2 = getVal(fldname);
			setVal(fldname, val1);
			super.moveToId(id1);
			setVal(fldname, val2);
		}
	}

	public boolean insertFromScan(Scan s) {
		if (!super.insert())
			return false;
		for (String fldname : sch.fields())
			setVal(fldname, s.getVal(fldname));
		return true;
	}

	public void copyToScan(UpdateScan s) {
		s.insert();
		for (String fldname : sch.fields())
			s.setVal(fldname, getVal(fldname));
	}

	// performs a selection sort
	public void internalSort(List<String> sortfields) {
		moveToId(-1);
		if (!next())
			return;
		int lhsId = currentId();
		while (true) {
			int minId = findSmallestFrom(lhsId, sortfields);
			if (minId != lhsId)
				swapRecords(minId, lhsId);
			moveToId(lhsId);
			if (!next())
				return;
			lhsId = currentId();
		}
	}

	public void printPage() {

		moveToId(0);
		while (true) {
			System.out.println("");
			for (String fldname : sch.fields()) {
				if (sch.type(fldname) == 4)
					System.out.print(getInt(fldname) + " ");
				else
					System.out.print(getString(fldname) + " ");
			}
			if (next() == false)
				break;
		}
		close();

		System.out.println("");
	}

	private int compareRecords(int id1, int id2, List<String> sortfields) {
		for (String fldname : sortfields) {
			super.moveToId(id1);
			Constant val1 = getVal(fldname);
			super.moveToId(id2);
			Constant val2 = getVal(fldname);
			int result = val1.compareTo(val2);
			if (result != 0)
				return result;
		}
		return 0;
	}

	private int compareRecords(int id1, Scan s, List<String> sortfields) {
		super.moveToId(id1);
		for (String fldname : sortfields) {
			Constant val1 = getVal(fldname);
			Constant val2 = s.getVal(fldname);
			int result = val1.compareTo(val2);
			if (result != 0)
				return result;
		}
		return 0;
	}
}
