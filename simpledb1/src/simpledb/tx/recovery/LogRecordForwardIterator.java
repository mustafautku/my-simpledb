package simpledb.tx.recovery;

import static simpledb.tx.recovery.LogRecord.CHECKPOINT;
import static simpledb.tx.recovery.LogRecord.COMMIT;
import static simpledb.tx.recovery.LogRecord.ROLLBACK;
import static simpledb.tx.recovery.LogRecord.SETINT;
import static simpledb.tx.recovery.LogRecord.SETSTRING;
import static simpledb.tx.recovery.LogRecord.START;

import java.util.Iterator;
import java.util.function.Consumer;

import simpledb.log.BasicLogRecord;
import simpledb.server.SimpleDB;

public class LogRecordForwardIterator implements Iterator<LogRecord> {
	private Iterator<BasicLogRecord> iter = SimpleDB.logMgr().forwardIterator();
	   
	   public boolean hasNext() {
	      return iter.hasNext();
	   }

	@Override
	public void forEachRemaining(Consumer<? super LogRecord> arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public LogRecord next() {
		// TODO Auto-generated method stub
		BasicLogRecord rec = iter.next();
	      int op = rec.nextInt();
	      switch (op) {
	         case CHECKPOINT:
	            return new CheckpointRecord(rec);
	         case START:
	            return new StartRecord(rec);
	         case COMMIT:
	            return new CommitRecord(rec);
	         case ROLLBACK:
	            return new RollbackRecord(rec);
	         case SETINT:
	            return new SetIntRecord(rec);
	         case SETSTRING:
	            return new SetStringRecord(rec);
	         default:
	            return null;
	      }
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
}
