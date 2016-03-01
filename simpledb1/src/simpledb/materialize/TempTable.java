package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.buffer.PageFormatter;
import simpledb.query.*;

/**
 * A class that creates temporary tables. A temporary table is not registered in
 * the catalog. The class therefore has a method getTableInfo to return the
 * table's metadata.
 * 
 * @author Edward Sciore
 */
public class TempTable {
	private static int nextTableNum = 0;
	private TableInfo ti;
	private Transaction tx;

	/**
	 * Allocates a name for for a new temporary table having the specified
	 * schema.
	 * 
	 * @param sch
	 *            the new table's schema
	 * @param tx
	 *            the calling transaction
	 */
	/**
	 * General usage of temp file. Do not need to access each pages of file.
	 * @param sch
	 * @param tx
	 */
	public TempTable(Schema sch, Transaction tx) {  
		String tblname = nextTableName();
		ti = new TableInfo(tblname, sch);
		this.tx = tx;
		/*
		 * TempTable' ilk bloðu formatlanmýþ olarak ekliyor. MBSort'da
		 * RecordPAge seviyesinde eriþmek için yaptýk. Evvelden temp
		 * dosyasýndaki RecordPage'lara eriþmiyorduk. TableScan(RecordFile)
		 * gerekli formatlamalarý yapýyordu.
		 */
		PageFormatter fmtr = new RecordFormatter(ti);
		tx.append(ti.fileName(), fmtr);
	}
	
	/**
	 * used for "formatted" CHUNK area including "size" number of pages. You can scan/update the chunk records within each specific recordpage.
	 * @param sch
	 * @param size
	 * @param tx
	 */
	  
	public TempTable(Schema sch, int size, Transaction tx) {
		String tblname = nextTableName();
		ti = new TableInfo(tblname, sch);
		this.tx = tx;
		// TempTable'a page'ler formatlanmýþ olarak ekleniyor.
		PageFormatter fmtr = new RecordFormatter(ti);
		for (int i = 0; i < size; i++)
			tx.append(ti.fileName(), fmtr);
	}

	/**
	 * Opens a table scan for the temporary table.
	 */
	public UpdateScan open() {
		return new TableScan(ti, tx);
	}

	/**
	 * Return the table's metadata.
	 * 
	 * @return the table's metadata
	 */
	public TableInfo getTableInfo() {
		return ti;
	}

	private static synchronized String nextTableName() {
		nextTableNum++;
		return "temp" + nextTableNum;
	}
}