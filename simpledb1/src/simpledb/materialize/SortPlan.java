package simpledb.materialize;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;

import java.util.*;

/**
 * The Plan class for the <i>sort</i> operator.
 * @author Edward Sciore
 */
public class SortPlan implements Plan {
   private Plan p;
   private Transaction tx;
   private Schema sch;
   private RecordComparator comp;
   private List<String> sortfields;
   
   
   /**
    * Creates a sort plan for the specified query.
    * @param p the plan for the underlying query
    * @param sortfields the fields to sort by
    * @param tx the calling transaction
    */
   public SortPlan(Plan p, List<String> sortfields, Transaction tx) {
      this.p = p;
      this.tx = tx;
      sch = p.schema();
      comp = new RecordComparator(sortfields);
      this.sortfields=sortfields;
   }
   
   /**
    * This method is where most of the action is.
    * Up to 2 sorted temporary tables are created,
    * and are passed into SortScan for final merging.
    * @see simpledb.query.Plan#open()
    */
	public Scan open() {
		Scan src = p.open();
		List<TempTable> runs = null;
		int splitK = SimpleDB.ExtSortParameters.splitK;
		int mergeK = SimpleDB.ExtSortParameters.mergeK;
		if (splitK == 0)
			runs = splitIntoRuns(src);
		else
			runs = splitIntoRunsWithKpages(src, splitK);
		src.close();
		if (mergeK == 1) {
			while (runs.size() > 2)
				runs = doAMergeIteration(runs);
			return new SortScan2way(runs, comp);
		} else {
			while (runs.size() > mergeK)
				runs = doAKwayMergeIteration(runs, mergeK);
			return new SortScanKway(runs, comp);
		}
		// return new SortScan(runs, comp);
//		return null;
	}
   
   /**
    * Returns the number of blocks in the sorted table,
    * which is the same as it would be in a
    * materialized table.
    * It does <i>not</i> include the one-time cost
    * of materializing and sorting the records.
    * @see simpledb.query.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      // does not include the one-time cost of sorting
      Plan mp = new MaterializePlan(p, tx); // not opened; just for analysis
      return mp.blocksAccessed();
   }
   
   /**
    * Returns the number of records in the sorted table,
    * which is the same as in the underlying query.
    * @see simpledb.query.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }
   
   /**
    * Returns the number of distinct field values in
    * the sorted table, which is the same as in
    * the underlying query.
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }
   
   /**
    * Returns the schema of the sorted table, which
    * is the same as in the underlying query.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
   
   private List<TempTable> splitIntoRuns(Scan src) {
      List<TempTable> temps = new ArrayList<TempTable>();
      src.beforeFirst();
      if (!src.next())
         return temps;
      TempTable currenttemp = new TempTable(sch, tx);
      temps.add(currenttemp);
      UpdateScan currentscan = currenttemp.open();
      while (copy(src, currentscan))
         if (comp.compare(src, currentscan) < 0) {
         // start a new run
         currentscan.close();
         currenttemp = new TempTable(sch, tx);
         temps.add(currenttemp);
         currentscan = (UpdateScan) currenttemp.open();
      }
      currentscan.close();
      return temps;
   }
   
   private List<TempTable> splitIntoRunsWithKpages(Scan src, int splitK) { // for now K=1
		List<TempTable> temps = new ArrayList<TempTable>();
		src.beforeFirst();
		TempRecordPage rp = null;
		boolean ok = src.next();
		while (ok) {
			TempTable temp = new TempTable(sch, tx);
			temps.add(temp);
			TableInfo ti = temp.getTableInfo();
			rp = new TempRecordPage(ti, 0, tx);
			while (rp.insertFromScan(src))
				if (!src.next()) {
					ok = false;
					break;
				}
			rp.internalSort(sortfields);
//			rp.printPage();
			rp.close();
		}
		if (rp != null)
			rp.close();
		return temps;
	}

   
   private List<TempTable> doAMergeIteration(List<TempTable> runs) {
      List<TempTable> result = new ArrayList<TempTable>();
      while (runs.size() > 1) {
         TempTable p1 = runs.remove(0);
         TempTable p2 = runs.remove(0);
         result.add(mergeTwoRuns(p1, p2));
      }
      if (runs.size() == 1)
         result.add(runs.get(0));
      return result;
   }
   
   private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
      Scan src1 = p1.open();
      Scan src2 = p2.open();
      TempTable result = new TempTable(sch, tx);
      UpdateScan dest = result.open();
      
      boolean hasmore1 = src1.next();
      boolean hasmore2 = src2.next();
      while (hasmore1 && hasmore2)
         if (comp.compare(src1, src2) < 0)
         hasmore1 = copy(src1, dest);
      else
         hasmore2 = copy(src2, dest);
      
      if (hasmore1)
         while (hasmore1)
         hasmore1 = copy(src1, dest);
      else
         while (hasmore2)
         hasmore2 = copy(src2, dest);
      src1.close();
      src2.close();
      dest.close();
      return result;
   }
   /*
	 * utku: 1 iteration of Merge: merge k runs at each step. Inýtýally, say, R runs
	 * exist, after the iteration ceiling(R/K) runs are collected.
	 */
	private List<TempTable> doAKwayMergeIteration(List<TempTable> runs,int mergeK) {
		List<TempTable> result = new ArrayList<TempTable>();
		while (runs.size() >= mergeK) {
			List<TempTable> Kruns = new ArrayList<TempTable>(mergeK);
			for (int i = 0; i < mergeK; i++)
				Kruns.add(runs.remove(0));
			result.add(mergeKRuns(Kruns));
		}
		if(runs.size()>0)
			result.add(mergeKRuns(runs)); // remaining <_kway runs.
		return result;
	}

	//utku
	private TempTable mergeKRuns(List<TempTable> Kruns) {
		List<Scan> _scanlist = new ArrayList<Scan>(); // all scans are opened
														// and first records are
														// pointed.
		for (int i = 0; i < Kruns.size(); i++) {
			TempTable tt = Kruns.get(i);
			Scan s = tt.open();
			_scanlist.add(s);
			s.next();
		}

		TempTable result = new TempTable(sch, tx);
		UpdateScan dest = result.open();
		while (true) {
			Iterator<Scan> it = _scanlist.iterator();
			List<Scan> _minscanlist = new ArrayList<Scan>(Kruns.size()); // used
																			// for
																			// min_list
			Scan _minScan;
			if (it.hasNext())
				_minScan = it.next();
			else
				break;
			_minscanlist.add(_minScan);
			while (it.hasNext()) {
				Scan _currentScan = it.next();
				int _state = comp.compare(_currentScan, _minScan);
				if (_state < 0) {
					_minScan = _currentScan;
					_minscanlist.clear();
					_minscanlist.add(_minScan);
				} else if (_state == 0) {
					_minscanlist.add(_currentScan);
				}
			}
			copy(_scanlist, _minscanlist, dest);
		}
		dest.close();
		return result;
	}
	
   private boolean copy(Scan src, UpdateScan dest) {
      dest.insert();
      for (String fldname : sch.fields())
         dest.setVal(fldname, src.getVal(fldname));
      return src.next();
   }
   
   //utku
   private void copy(List<Scan> scans, List<Scan> mins, UpdateScan dest) {
		Iterator<Scan> it = mins.iterator();
		while (it.hasNext()) {
			dest.insert();
			Scan src = it.next();
			for (String fldname : sch.fields())
				dest.setVal(fldname, src.getVal(fldname));
			if (src.next() == false) { // close the scan and remove from the
										// main list.
				scans.remove(src);
				src.close();
			}
		}
		mins.clear();
	}
}
