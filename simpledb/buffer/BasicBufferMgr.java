package simpledb.buffer;

import java.util.HashMap;
import java.util.HashSet;

import simpledb.file.Block;

public class BasicBufferMgr {

	private int bufferCountLimit;
	private BufferNode head,tail;
	private HashMap<Block,Buffer> bufferPoolMap;
	
	public BasicBufferMgr(int poolSize){
		// to have a limit on bufferNode
		bufferCountLimit = poolSize;
		bufferPoolMap = new HashMap<Block,Buffer>();
		head=null;
		tail=null;
	}
	
	public Boolean enqueue(Buffer item){
		if(bufferCountLimit==0)
			return false;
		if(tail==null){
			tail=new BufferNode(item);
			tail.setNext(null);
			head=tail;
		}else{
			BufferNode temp = new BufferNode(item);
			temp.setNext(null);
			tail.setNext(temp);
			tail=temp;
		}
		if(item.block()!=null)
			bufferPoolMap.put(item.block(), item);
		return true;
	}
	
	public Buffer dequeue(){
		BufferNode ptr = head,prevPtr=head;
		while((ptr!=null)&&(ptr.getVal().isPinned())){
			prevPtr = ptr;
			ptr = ptr.getNext();
		}
		
		if(ptr!=null){
			//if(bufferCountLimit<8)
			//	bufferCountLimit++;
			if(head==ptr){
				if(head.getNext()!=null)
					head = head.getNext();
				else{
					head = null;
					tail=null;
				}
			}else
				prevPtr.setNext(ptr.getNext());
			
			bufferPoolMap.remove(ptr.getVal().block());
			return ptr.getVal();
		}
		
		return null;
	}
	
	/**
	* Flushes the dirty buffers modified by the specified transaction.
	* @param txnum the transaction's id number
	*/
	synchronized void flushAll(int txnum) {
		BufferNode temp = tail;
		while((temp!= head)&&(temp!=null)){
			if (temp.getVal().isModifiedBy(txnum))
				temp.getVal().flush();
			temp=temp.getNext();
		}
	}
	
	 /**
	 * Pins a buffer to the specified block. 
	 * If there is already a buffer assigned to that block
	 * then that buffer is used;  
	 * otherwise, an unpinned buffer from the pool is chosen.
	 * Returns a null value if there are no available buffers.
	 * @param blk a reference to a disk block
	 * @return the pinned buffer
	 */
	 synchronized Buffer pin(Block blk) {
	   Buffer buff = findExistingBuffer(blk);
	   if (buff == null) {
	      buff = chooseUnpinnedBuffer();
	      if (buff == null)
	         return null;
	      bufferPoolMap.put(blk, buff);
	      buff.assignToBlock(blk);
	   }
	   if (!buff.isPinned())
		   bufferCountLimit--;
	   
	   buff.pin();
	   return buff;
	}
	
    /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
	   Buffer buff = chooseUnpinnedBuffer();
	   if (buff == null){
	      return null;
	   }
	   buff.assignToNew(filename, fmtr);
	   bufferPoolMap.put(buff.block(), buff);
	   bufferCountLimit--;
	   buff.pin();
	   return buff;
	}
	
	/**
	* Unpins the specified buffer.
	* @param buff the buffer to be unpinned
	*/
	synchronized void unpin(Buffer buff) {
	   buff.unpin();
	   if (!buff.isPinned())
		   bufferCountLimit++;
	}
	   
	/**
	* Returns the number of available (i.e. unpinned) buffers.
	* @return the number of available buffers
	*/
	int available() {
	   return bufferCountLimit;
	}

	
	private Buffer findExistingBuffer(Block blk) {
		return bufferPoolMap.get(blk);
	}
	
	boolean containsMapping(Block blk) {
		return bufferPoolMap.containsKey(blk);
	}
		   
	Buffer getMapping(Block blk) {
		return bufferPoolMap.get(blk);
	}
	
    private Buffer chooseUnpinnedBuffer() {
       Buffer temp =  dequeue();
       if(temp!=null){
    	   bufferPoolMap.remove(temp.block());
       }else if(bufferCountLimit>0){
    	   temp = new Buffer();
    	   enqueue(temp);
       }
       return temp;
    }
}
