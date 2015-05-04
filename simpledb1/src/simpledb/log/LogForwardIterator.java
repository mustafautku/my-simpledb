package simpledb.log;

import static simpledb.file.Page.INT_SIZE;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import simpledb.file.Block;
import simpledb.file.FileMgr;
import simpledb.file.Page;

/**
 * A class that provides the ability to move through the
 * records of the log file in reverse order.
 * 
 * @author Edward Sciore
 */
class LogForwardIterator implements Iterator<BasicLogRecord>{
	private Block blk;
	private Page pg = new Page();
	private int currentrec;
	private ArrayList<Integer> records = new ArrayList<Integer>();
	private int index = 0;

	/**
	 * Creates an iterator for the records in the log file,
	 * positioned after the last log record.
	 * This constructor is called exclusively by
	 * {@link LogMgr#iterator()}.
	 */
	LogForwardIterator(Block blk){
		this.blk = blk;
		pg.read(blk);
		currentrec = pg.getInt(LogMgr.LAST_POS);
		while(currentrec != 0){
			currentrec = pg.getInt(currentrec);
			records.add(currentrec + INT_SIZE);
		}
		Collections.reverse(records);
	}

	/**
	 * Determines if the current log record
	 * is the earliest record in the log file.
	 * @return true if there is an earlier record	
	 */
	public boolean hasNext(){
		return (currentrec != records.get(records.size() - 1)) && (currentrec < Page.BLOCK_SIZE || blk.number() < FileMgr.getSize(blk.fileName()));
	}

	/**
	 * Moves to the next log record in reverse order.
	 * If the current log record is the earliest in its block,
	 * then the method moves to the next oldest block,
	 * and returns the log record from there.
	 * @return the next earliest log record
	 */
	public BasicLogRecord next(){
		if(currentrec == Page.BLOCK_SIZE)
			moveToNextBlock();
		//currentrec = pg.getInt(currentrec);
		currentrec = records.get(index);
		if(index < records.size() - 1){
			index++;
		}
		return new BasicLogRecord(pg, currentrec);
	}

	public void remove(){
		throw new UnsupportedOperationException();
	}

	/**
	 * Moves to the next log block in reverse order,
	 * and positions it after the last record in that block.
	 */
	private void moveToNextBlock(){
		blk = new Block(blk.fileName(), blk.number() + 1);
		pg.read(blk);
		currentrec = pg.getInt(0);
	}
}
