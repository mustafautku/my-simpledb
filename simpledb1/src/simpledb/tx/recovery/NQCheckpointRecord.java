package simpledb.tx.recovery;

import java.util.ArrayList;
import java.util.List;
import simpledb.log.BasicLogRecord;

/**
 * The NQCHECKPOINT log record.
 * @author Edward Sciore
 */
class NQCheckpointRecord implements LogRecord{

	private List<Integer> mylist = new ArrayList<Integer>();

	/**
	 * Creates a quiescent checkpoint record.
	 */
	public NQCheckpointRecord(List<Integer> list){
		this.mylist = list;
	}

	/**
	 * Creates a log record by reading no other values 
	 * from the basic log record.
	 * @param rec the basic log record
	 */
	public NQCheckpointRecord(BasicLogRecord rec){
	}

	/** 
	 * Writes a checkpoint record to the log.
	 * This log record contains the CHECKPOINT operator,
	 * and nothing else.
	 * @return the LSN of the last log value
	 */
	public int writeToLog(){
		Object[] rec = new Object[]{ NQCHECKPOINT };
		return logMgr.append(rec);
	}

	public int op(){
		return NQCHECKPOINT;
	}

	/**
	 * Checkpoint records have no associated transaction,
	 * and so the method returns a "dummy", negative txid.
	 */
	public int txNumber(){
		return -1; // dummy value
	}

	/**
	 * Does nothing, because a checkpoint record
	 * contains no undo information.
	 */
	public void undo(int txnum){
	}

	public String toString(){ // <NQCHECKPOINT 1,2,3,4> gibi yazmalý
		String record = "<NQCHECKPOINT ";

		for (int i = 0; i < mylist.size() - 1; i++){
			record = record + mylist.get(i) + ", ";
		}
		record += mylist.get(mylist.size());
		record += ">";
		return record;
	}
}
