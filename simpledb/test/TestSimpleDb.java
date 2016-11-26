package simpledb.test;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

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
	
	private static Boolean testCase1(){
		return true;
	}
	
	public static void main(String[] args) {
		int testCaseNum=0;
		try{
			startup();
			
			// Start test cases
			if(testCase1())
				testCaseNum++;
			
			teardown();
			System.out.println("All test cases ran successfully");
		}catch(Exception e){
			System.out.println("Failed test case ["+testCaseNum+"]");
			e.printStackTrace();
		}

	}

}
