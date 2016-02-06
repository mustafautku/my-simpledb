package simpledb.server;
import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.server.SimpleDB;
import simpledb.server.SimpleDB.PlannerType;

/* This is a version of the StudentMajor program that
 * accesses the SimpleDB classes directly (instead of
 * connecting to it as a JDBC client).  You can run it
 * without having the server also run.
 * 
 * These kind of programs are useful for debugging
 * your changes to the SimpleDB source code.
 */

/**
 *  Unimportant minor changes, like SQL statement submitted.
 * @author mustafautku
 *
 */

public class StudentMajorNoServer {
	public static void main(String[] args) {
		try {
			// analogous to the driver
			SimpleDB.init("studentdb");
			
			// analogous to the connection
			Transaction tx = new Transaction();
			
			// analogous to the statement
			String qry = "select sname, majorid "
		        + "from student ";	
			Plan p = SimpleDB.planner(PlannerType.BASIC).createQueryPlan(qry, tx);
			
			// analogous to the result set
			Scan s = p.open();
			
			System.out.println("Name\tMajor");
			while (s.next()) {
				String sname= s.getString("sname"); //SimpleDB stores field names
				int did = s.getInt("majorid"); //in lower case
				System.out.println(did + "\t" + sname);
			}
			s.close();
			tx.commit();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
