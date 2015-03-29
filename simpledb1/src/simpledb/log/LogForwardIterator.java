package simpledb.log;

import static simpledb.file.Page.INT_SIZE;

import java.util.*;

import simpledb.buffer.Buffer;
import simpledb.file.Block;
import simpledb.file.Page;
import simpledb.server.SimpleDB;

class LogForwardIterator implements Iterator<BasicLogRecord> {
	private Block blk;
	private Page pg = new Page();
	private int currentblock = 0;
	private int filesize;
//	private int currentrecord = 0;
	private Stack<Integer> logOffsets=new Stack<Integer>();

	/**
	 * Creates an iterator for the records in the log file, positioned after the
	 * last log record. This constructor is called exclusively by
	 * {@link LogMgr#iterator()}.
	 */
	LogForwardIterator() {
		filesize=SimpleDB.fileMgr().size(SimpleDB.LOG_FILE);
		analyzeBlock();
		
	}

	/**
	 * Determines if the current log record is the earliest record in the log
	 * file.
	 * 
	 * @return true if there is an earlier record
	 */
	public boolean hasNext() {
		return !logOffsets.empty() || currentblock< filesize;
	}

	/**
	 * Moves to the next log record in reverse order. If the current log record
	 * is the earliest in its block, then the method moves to the next oldest
	 * block, and returns the log record from there.
	 * 
	 * @return the next earliest log record
	 */
	public BasicLogRecord next() {
		int pos;
		if(!logOffsets.empty()){
			pos=logOffsets.pop();
			return new BasicLogRecord(pg, pos + INT_SIZE);
		}			
		analyzeBlock();
		if(!logOffsets.empty()){
			pos=logOffsets.pop();
			return new BasicLogRecord(pg, pos + INT_SIZE);
		}	
		return null;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Moves to the next log block in reverse order, and positions it after the
	 * last record in that block.
	 */
	private void analyzeBlock() {
		this.blk = new Block(SimpleDB.LOG_FILE, currentblock++);
		pg.read(blk);

		int pos = pg.getInt(LogMgr.LAST_POS);
		while (pos > 0) {
			int newpos = pg.getInt(pos);
			logOffsets.push(newpos);
			pos = newpos;
		}
	}
}
