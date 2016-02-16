package testextsort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import simpledb.materialize.Split1Merge2SortPlan;
import simpledb.query.Plan;
import simpledb.query.ProjectPlan;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class Split1Merge2Test2 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SimpleDB.BUFFER_SIZE=4;  // This the minumum buffer pool size required. 2 for inputs + 2 for output file.
		ServerInit1Table.initData("testExtSort");
		Transaction tx = new Transaction();
		Plan p1 = new TablePlan("student", tx);
		List<String> sf = Arrays.asList("sid");
		Plan p2 = new ProjectPlan(p1, sf);
		Scan s = p2.open();
		while (s.next())
			System.out.println(s.getInt("sid") + " ");
		s.close();
		System.out.println("END OF FILE");

		Split1Merge2SortPlan mbsp = new Split1Merge2SortPlan(p1, sf, tx);
		s = mbsp.open();
		int counter=0;
		int samerecordscounter=0;
		int prevsid=-1;
		while (s.next()) {
			int id = s.getInt("sid");
			
			System.out.println(id);
			if(prevsid == id)
				samerecordscounter++;
			prevsid = id;
			counter++;
		}
		System.out.println("SUCCESS:" + counter + " records are sorted."
				+ samerecordscounter + " are repetitions.");
	}

}
