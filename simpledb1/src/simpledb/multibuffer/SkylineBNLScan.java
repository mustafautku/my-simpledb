package simpledb.multibuffer;

import java.util.ArrayList;
import java.util.List;

import simpledb.tx.Transaction;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.materialize.TempTable;
import simpledb.query.*;

/** 
 * The Scan class for the muti-buffer version of the
 * <i>product</i> operator.
 * @author Edward Sciore
 */
public class SkylineBNLScan implements Scan {

   private Transaction tx;
   private Schema sch;
   private Scan input;
   private ChunkScan window;
   private TableScan output;

   private ArrayList<RID> notSkylineList;
   private SkylineRecordComparator comp;

   private boolean tempfileExist=false;
   
   
   /**
    * Creates the scan class for the product of the LHS scan and a table.
    * @param lhsscan the LHS scan
    * @param ti the metadata for the RHS table
    * @param tx the current transaction
    */
   public SkylineBNLScan(Scan input,ChunkScan window,TableScan output, Schema sch,List<String> skylinefields, Transaction tx) {
      this.tx = tx;
      this.input=input;
      this.window=window;
      this.output=output;

      comp = new SkylineRecordComparator(skylinefields);
      this.sch=sch; 
      
      beforeFirst();
   }
   
   /**
    * Positions the scan before the first record.
    * That is, the LHS scan is positioned at its first record,
    * and the RHS scan is positioned before the first record of the first chunk.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {

	   tempfileExist=false;
	   doAnIteration(input,output);
   }
   /**
    * Moves to the next record in the current scan.
    * If there are no more records in the current chunk,
    * then move to the next LHS record and the beginning of that chunk.
    * If there are no more LHS records, then move to the next chunk
    * and begin again.
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
	   
	   window.beforeFirst();

		while (window.next()) {
			if(!notSkylineList.contains(window.getRid())){
				return true;
			}			
		}
		
		if (tempfileExist) {
			input = output;
			TempTable outputfile = new TempTable(sch, tx);
			output = (TableScan) outputfile.open();

			doAnIteration(input, output);
			return next();
		}
		return false;
   }
   private void doAnIteration(Scan input, TableScan output){
		input.beforeFirst();
		
		tempfileExist=false;
		notSkylineList=new ArrayList<RID>();
		
		while (input.next()) {
			window.beforeFirst();
			boolean willBeInwindow = true;
			
//			int myTest=-1; // for debug
			while (window.next()) {
				int sky=comp.compare(input,window);
				if(sky==1){
					window.delete();
//					myTest=0;// for debug
				}
				else if(sky ==2){
//					if(myTest ==0) System.err.print("impossible! error");// for debug
					willBeInwindow = false;
					break;
				}
			}
			if (willBeInwindow) {
				window.beforeFirst();
				if (window.insertFromScan(input)) {
					RID wRID=window.getRid();
					if(tempfileExist && !notSkylineList.contains(wRID))
						notSkylineList.add(wRID);
				} else {  // window is full. Should write to output file(temp)
					tempfileExist=true;
					transferBwScans(input,output);
				}
			}
		}	
		input.close();
	}
   
   
   /**
    * Closes the current scans.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
	   input.close();
      window.close();
      output.close();
   }
   
   /** 
    * Returns the value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      Constant skylineRecordvalue= window.getVal(fldname);
      window.delete();
      return skylineRecordvalue;
   }
   
   /** 
    * Returns the integer value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public int getInt(String fldname) {
      int skylineRecordvalue=window.getInt(fldname);
      window.delete();
      return skylineRecordvalue;
   }
   
   /** 
    * Returns the string value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public String getString(String fldname) {
      String skylineRecordvalue= window.getString(fldname);
      window.delete();
      return skylineRecordvalue;
   }
   
   /**
    * Returns true if the specified field is in
    * either of the underlying scans.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return window.hasField(fldname);
   }
   
   

@Override
	public double getDouble(String fldname) {
		double skylineRecordvalue = window.getDouble(fldname);
		window.delete();
		return skylineRecordvalue;
	}

	public boolean transferBwScans(Scan s1, UpdateScan s2) {
		s2.insert();
		for (String fldname : sch.fields())
			s2.setVal(fldname, s1.getVal(fldname));
		return true;
	}

}

