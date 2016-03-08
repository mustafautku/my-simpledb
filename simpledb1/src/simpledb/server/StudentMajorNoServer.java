package simpledb.server;
import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.server.SimpleDB;

/* This is a version of the StudentMajor program that
 * accesses the SimpleDB classes directly (instead of
 * connecting to it as a JDBC client).  You can run it
 * without having the server also run.
 * 
 * These kind of programs are useful for debugging
 * your changes to the SimpleDB source code.
 */

public class StudentMajorNoServer {
	public static void main(String[] args) {
		try {
			// analogous to the driver
			SimpleDB.init("skylineDouble");
			
			// analogous to the connection
			Transaction tx = new Transaction();
			
			// analogous to the statement
			String qry = "select a,b "
		        + "from input where a=0,0581380409841231";
//		        + "where MajorId = DId";	
			Plan p = SimpleDB.planner().createQueryPlan(qry, tx);
			
			// analogous to the result set
			Scan s = p.open();
			
//			System.out.println("Name\tMajor");
//			while (s.next()) {
//				String sname = s.getString("sname"); //SimpleDB stores field names
//				String dname = s.getString("dname"); //in lower case
//				System.out.println(sname + "\t" + dname);
//			}
			
			System.out.println("A\tB");
			while (s.next()) {
				double A = s.getDouble("a"); 
				double B = s.getDouble("b"); 
				System.out.println(A + "\t" + B);
			}
			
//			System.out.println("A\tB");
//			while (s.next()) {
//				int A = s.getInt("a"); 
//				int B = s.getInt("b"); 
//				System.out.println(A + "\t" + B);
//			}
			
			s.close();
			tx.commit();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
