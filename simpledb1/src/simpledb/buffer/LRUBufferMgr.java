package simpledb.buffer;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import simpledb.file.Block;

/**
 * 
 * @author Eyüp İBİŞOĞLU
 *
 */
public class LRUBufferMgr
{
	private Queue<Buffer> unpinnedBuffers;
	private Map<Block, Buffer> pinnedBuffers;
	
	public LRUBufferMgr(int size)
	{
		unpinnedBuffers = new PriorityQueue<Buffer>(size);
		
		for (int i = 0; i < size; i++)
		{
			Buffer buffer = new Buffer();
			buffer.setId( i );
			unpinnedBuffers.add( buffer );
		}
		
		pinnedBuffers = new HashMap<Block, Buffer>(size);
	}
	
	
	
	@Override
	public String toString()
	{
		StringBuilder text = new StringBuilder();
		
		text.append( "*** Unpinned Buffers ***\n" );
		for (Buffer buffer : unpinnedBuffers)
			text.append( buffer.toString() + "\n\n");
		
		text.append( "*** Pinned Buffers ***\n" );
		for (Buffer buffer : pinnedBuffers.values())
			text.append( buffer.toString() + "\n\n");
		
		return text.toString();
	}



	synchronized void flushAll(int txnum) 
	{
		for (Buffer buffer : unpinnedBuffers)
			if (buffer.isModifiedBy(txnum))
				buffer.flush();

		for (Buffer buffer : pinnedBuffers.values())
			if (buffer.isModifiedBy(txnum))
				buffer.flush();
	}
	
	synchronized Buffer pin(Block block) 
	{
    	  Buffer buffer = chooseUnpinnedBuffer();
    	  
    	  if (buffer == null)
    		  return null;
    	  
          buffer.assignToBlock( block );
	      buffer.pin();
	      pinnedBuffers.put(buffer.block(), buffer);
	      
	      return buffer;
	}
	
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) 
	{
	      Buffer buffer = chooseUnpinnedBuffer();
	      
	      if (buffer == null)
	         return null;
	      
	      buffer.assignToNew(filename, fmtr);
	      buffer.pin();
	      pinnedBuffers.put(buffer.block(), buffer);
	      
	      return buffer;
    }
	
	synchronized void unpin(Buffer buffer) 
	{
	      buffer.unpin();
	      pinnedBuffers.remove( buffer.block() );
	      unpinnedBuffers.add( buffer );
	}
	
	int available()
	{
		return unpinnedBuffers.size();
	}
	
	private Buffer findExistingBuffer(Block block)
	{
		return pinnedBuffers.get( block );
	}
	
	private Buffer chooseUnpinnedBuffer() 
	{
	      return unpinnedBuffers.poll();
	}
}
