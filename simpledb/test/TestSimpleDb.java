package simpledb.test;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.server.SimpleDB;

/**
 *	Test class containing all test cases
 */

public class TestSimpleDb {
	private static void startup(){
		SimpleDB.init("simpleDB");
	}
	
	private static void teardown(){
		
	}
	
	private static Boolean testBufferAllocation() throws Exception{
		System.out.println("Testcase : Buffer Normal allocations");
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr() ;
		try {
			for(int i=0;i<5;i++){
				Block blk1 = new Block("filename", i);
				basicBufferMgr.pin(blk1);
			}
		}
		catch (BufferAbortException e) {
			System.out.println("Testcase : Buffer Normal allocations : FAIL");
			throw new Exception("Buffer overflow not expected");
		}
		
		System.out.println("Testcase : Buffer Normal allocations : PASS");
		return true;
	}
	
	private static Boolean testBufferOverflow() throws Exception{
		System.out.println("Testcase : Buffer overflow");
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr() ;
		for(int i=0;i<8;i++){
			Block blk1 = new Block("filename", i);
			basicBufferMgr.pin(blk1);
		}
		Block blk1 = new Block("filename", 8);
		try {
			basicBufferMgr.pin(blk1);
		}
		catch (BufferAbortException e) {
			System.out.println("Testcase : Buffer overflow : PASS");
			return true;
		}
		System.out.println("Testcase : Buffer overflow : FAIL");
		throw new Exception("Buffer overflow expected");
	}
	
	private static Boolean testFIFO() throws Exception{
		System.out.println("Testcase : FIFO insertion");
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr() ;
		Block[] blkList = new Block[8];
		for(int i=0;i<8;i++){
			Block temp = new Block("filename", i);
			blkList[i]=temp;
			basicBufferMgr.pin(temp);
		}
		
		Buffer unpinnedBuff=basicBufferMgr.getMapping(blkList[2]);
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blkList[3]));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blkList[2]));
		
		//Block temp = new Block("filename", 8);
		//basicBufferMgr.pin(temp);
		
		if(basicBufferMgr.getBufferMgr().dequeue()==unpinnedBuff){
			System.out.println("Testcase : FIFO insertion : PASS");
			return true;
		}
		
		
		System.out.println("Testcase : FIFO insertion : FAILED");
		throw new Exception("FIFO not expected buffer ");
	}
	
	public static void main(String[] args) {
		int testCaseNum=0;
		try{
			startup();
			
			// Start test cases
			testFIFO();
			testBufferAllocation();
			testBufferOverflow();
			
			
			
			
			teardown();
			System.out.println("All test cases ran successfully");
		}catch(Exception e){
			System.out.println("Passed test cases ["+testCaseNum+"]");
		}

	}

}
