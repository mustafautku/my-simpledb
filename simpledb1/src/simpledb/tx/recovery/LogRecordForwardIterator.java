package simpledb.tx.recovery;

import static simpledb.tx.recovery.LogRecord.*;
import java.util.Iterator;
import java.util.Stack;

import simpledb.log.BasicLogRecord;
import simpledb.server.SimpleDB;

public class LogRecordForwardIterator implements Iterator<LogRecord> {
   private Stack<LogRecord> logstack;

   public LogRecordForwardIterator() {
      logstack = new Stack<LogRecord>();
      Iterator<LogRecord> iter = new LogRecordIterator();

      while (iter.hasNext()) {
         LogRecord item = iter.next();
         logstack.push(item);
      }
   }

   public boolean hasNext() {
      return !logstack.empty();
   }

   public LogRecord next() {
      return logstack.pop();
   }

   public void remove() {
      throw new UnsupportedOperationException();
   }
}
