package simpledb.materialize;

import java.util.ArrayList;
import java.util.List;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.record.TempRecordPage;
import simpledb.tx.Transaction;


/**
 *  1 page splits are generated with using only 2 buffers.
 *  2 way merge can be done with using only 4 buffers. 2 output buffers are required for extension.
 * @author mustafautku
 *
 */
public class Split1Merge2SortPlan implements Plan {

	private Plan p;
	private Transaction tx;
	private Schema sch;
	private RecordComparator comp;
	List<String> sortfields;

	public Split1Merge2SortPlan(Plan p, List<String> sortfields, Transaction tx) {
		this.p = p;
		this.tx = tx;
		sch = p.schema();
		comp = new RecordComparator(sortfields);
		this.sortfields=sortfields;
	}

	@Override
	public Scan open() {
		Scan src = p.open();
		List<TempTable> runs = splitIntoRuns(src); // here we need only 2 buffers.
		src.close(); // we are done with src. Give up the buffer. 
		while (runs.size() > 2)
			runs = doAMergeIteration(runs); // here we need only 4 buffers. 2 for inputs, 2 for output.
		return new SortScan(runs, comp); 
	}

	@Override
	public int blocksAccessed() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int recordsOutput() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int distinctValues(String fldname) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Schema schema() {
		// TODO Auto-generated method stub
		return null;
	}
	private List<TempTable> splitIntoRuns(Scan src) {
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
			rp.printPage();
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
	   
	   private boolean copy(Scan src, UpdateScan dest) {
	      dest.insert();
	      for (String fldname : sch.fields())
	         dest.setVal(fldname, src.getVal(fldname));
	      return src.next();
	   }

}
