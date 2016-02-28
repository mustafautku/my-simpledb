package testextsort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import simpledb.materialize.SortPlan;
import simpledb.query.Plan;
import simpledb.query.ProjectPlan;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class ExtSortTest1 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SimpleDB.BUFFER_SIZE=8;  // This the minumum buffer pool size required. 2 for inputs + 2 for output file.
		ServerInit1Table.initData("testExtSort");
		
		int splitK,mergeK,repSelK;
		
//		System.out.println("**********************************");
//		splitK=SimpleDB.ExtSortParameters.splitK=0;
//		mergeK=SimpleDB.ExtSortParameters.mergeK=2;
//		repSelK=SimpleDB.ExtSortParameters.repSelK=0;  		
//		System.out.println("splitK: " + splitK + "\nmergeK: "+ mergeK + "\nrepSelK: "+ repSelK);
//		runExtSort();
		
		System.out.println("**********************************");
		splitK=SimpleDB.ExtSortParameters.splitK=1;
		mergeK=SimpleDB.ExtSortParameters.mergeK=2;
		repSelK=SimpleDB.ExtSortParameters.repSelK=0;  		
		System.out.println("splitK: " + splitK + "\nmergeK: "+ mergeK + "\nrepSelK: "+ repSelK);
		runExtSort();
		
//		System.out.println("**********************************");
//		splitK=SimpleDB.ExtSortParameters.splitK=-1;
//		mergeK=SimpleDB.ExtSortParameters.mergeK=2;
//		repSelK=SimpleDB.ExtSortParameters.repSelK=-1;  		
//		System.out.println("splitK: " + splitK + "\nmergeK: "+ mergeK + "\nrepSelK: "+ repSelK);
//		runExtSort();
//		
//		System.out.println("**********************************");
//		splitK=SimpleDB.ExtSortParameters.splitK=-1;
//		mergeK=SimpleDB.ExtSortParameters.mergeK=2;
//		repSelK=SimpleDB.ExtSortParameters.repSelK=1;  		
//		System.out.println("splitK: " + splitK + "\nmergeK: "+ mergeK + "\nrepSelK: "+ repSelK);
//		runExtSort();
//		
//		System.out.println("**********************************");
//		splitK=SimpleDB.ExtSortParameters.splitK=-1;
//		mergeK=SimpleDB.ExtSortParameters.mergeK=2;
//		repSelK=SimpleDB.ExtSortParameters.repSelK=3;  		
//		System.out.println("splitK: " + splitK + "\nmergeK: "+ mergeK + "\nrepSelK: "+ repSelK);
//		runExtSort();
//		
//		System.out.println("**********************************");
//		splitK=SimpleDB.ExtSortParameters.splitK=-1;
//		mergeK=SimpleDB.ExtSortParameters.mergeK=2;
//		repSelK=SimpleDB.ExtSortParameters.repSelK=5;  		
//		System.out.println("splitK: " + splitK + "\nmergeK: "+ mergeK + "\nrepSelK: "+ repSelK);
//		runExtSort();
//		
//		
//		System.out.println("**********************************");
//		splitK=SimpleDB.ExtSortParameters.splitK=-1;
//		mergeK=SimpleDB.ExtSortParameters.mergeK=4;
//		repSelK=SimpleDB.ExtSortParameters.repSelK=5;  		
//		System.out.println("splitK: " + splitK + "\nmergeK: "+ mergeK + "\nrepSelK: "+ repSelK);
//		runExtSort();
	}

	private static void runExtSort(){
		Transaction tx = new Transaction();
		Plan p1 = new TablePlan("student", tx);
		List<String> sf = Arrays.asList("sid");
//		Plan p2 = new ProjectPlan(p1, sf);
//		Scan s = p2.open();
//		while (s.next())
//			System.out.println(s.getInt("sid") + " ");
//		s.close();
//		System.out.println("END OF FILE");

		SortPlan mbsp = new SortPlan(p1, sf, tx);
		Scan s = mbsp.open();
		int counter=0;
		int samerecordscounter=0;
		int prevsid=-1;
		while (s.next()) {
			int id = s.getInt("sid");			
			System.out.println(id);
			if(prevsid == id)
				samerecordscounter++;
			else if(prevsid > id)
				System.err.print("ext sort FATAL ERROR!!....");
			prevsid = id;
			counter++;
		}
		s.close();
		tx.rollback();
		System.out.println("SUCCESS:" + counter + " records are sorted."
				+ samerecordscounter + " are repetitions.");
	}
}
