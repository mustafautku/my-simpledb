package simpledb.materialize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.SortScan;
import simpledb.query.TableScan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;

public class SortScanKway implements SortScan {

	UpdateScan currentscan=null;
	private RecordComparator comp;
	ArrayList<RID> _ridlist=null;
	ArrayList<RID> _savedridlist=null;
	ArrayList<UpdateScan> _scanlist=new ArrayList<UpdateScan>();
	int runsize;
	
	public SortScanKway(List<TempTable> runs, RecordComparator comp) {
	
		runsize=runs.size();
		_ridlist=new ArrayList<RID>(runsize);
		_savedridlist=new ArrayList<RID>(runsize);
		 this.comp = comp;
		 Iterator<TempTable> it=runs.iterator();
		 while(it.hasNext()){
			 TempTable tt=it.next();
			 UpdateScan s = (UpdateScan)tt.open();
			 if(s.next()) {
				 _scanlist.add(s);
				 _ridlist.add(s.getRid());  // RID: 0,0
				 _savedridlist.add(null);
			 }
			 else s.close();  
		 }
	}

	@Override
	public void beforeFirst() {
		currentscan = null;
		Iterator<UpdateScan> it=_scanlist.iterator();
		while(it.hasNext()){
			UpdateScan s=it.next();
			s.beforeFirst();		
			s.next();
		}		
	}
	
	public boolean next() {  
		if (currentscan != null) {	
			int idxOfscan=_scanlist.indexOf(currentscan);
			boolean hasmore=currentscan.next();
			
			if(hasmore)
				_ridlist.set(idxOfscan,currentscan.getRid());
			else{
				_ridlist.set(idxOfscan,null);
//				currentscan.close(); // do not close. Because may need to be restored.
				Iterator<RID> itv=_ridlist.iterator();
				boolean _cont=false;
				while(itv.hasNext())
					if(itv.next()!=null){_cont=true;break;}
				if(!_cont) return false;
				
			}		
	      }	
		setCurrentScan();
		return true;
	}
	private void setCurrentScan(){// this func set the currentscan.
		int i=0;
		for(i=0;i<runsize;i++){
			if(_ridlist.get(i) != null){ 
				currentscan=_scanlist.get(i);
				break;
			}
		}
		for(int j=i+1;j<runsize;j++){
			if(_ridlist.get(j) != null && comp.compare(currentscan, _scanlist.get(j))>0) 
				currentscan=_scanlist.get(j);		
		}
	}
	@Override
	public void close() {
		Iterator<UpdateScan> it=_scanlist.iterator();
		while(it.hasNext()){			
			it.next().close();			
		}
	}

	@Override
	public Constant getVal(String fldname) {
		return currentscan.getVal(fldname);
	}

	@Override
	public int getInt(String fldname) {
		return currentscan.getInt(fldname);
	}

	@Override
	public String getString(String fldname) {
		return currentscan.getString(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return currentscan.hasField(fldname);
	}

	/**
	 * Saves the position of the current record, so that it can be restored at a
	 * later time.
	 */
	public void savePosition() {		
		for(int i=0;i<runsize;i++){
			if(_ridlist.get(i) ==null){
				_savedridlist.set(i,null); 
			}
			if(_ridlist.get(i) !=null){
				RID r1=_ridlist.get(i);
				_savedridlist.set(i,new RID(r1.blockNumber(),r1.id()));  // clone
			}
		}
	}

	/**
	 * Moves the scan to its previously-saved position.
	 */
	public void restorePosition() {
		for(int i=0;i<runsize;i++){
			RID r1=_savedridlist.get(i);		
			if(r1 !=null){				
				_scanlist.get(i).moveToRid(r1);
				_ridlist.set(i,new RID(r1.blockNumber(),r1.id()));  // clone
			}
		}
		setCurrentScan();  // restore currentscan for the new scanlist.
	}
}
