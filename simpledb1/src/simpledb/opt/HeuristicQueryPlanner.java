package simpledb.opt;

import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.opt.TablePlanner;
import simpledb.parse.QueryData;
import simpledb.planner.QueryPlanner;
import java.util.*;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 * @author Edward Sciore
 */
public class HeuristicQueryPlanner implements QueryPlanner {
   private Collection<TablePlanner> tableplanners = new ArrayList<TablePlanner>();
   
   /**
    * Creates an optimized left-deep query plan using the following
    * heuristics.
    * H6B. Choose the table (considering selection predicates) which has the highest reduction factor
    * to be first in the join order.
    * H2. Add the table to the join order which
    * results in the "smallest output". Thus getLowestProductPlan is the same as 6A
    */
   public Plan createPlan(QueryData data, Transaction tx) {
      
      // Step 1:  Create a TablePlanner object for each mentioned table
      for (String tblname : data.tables()) {
         TablePlanner tp = new TablePlanner(tblname, data.pred(), tx);
         tableplanners.add(tp);
      }
      
      // Step 2:  Choose the lowest-size plan to begin the join order
      Plan currentplan = getLowestSelectPlan();
      
      // Step 3:  Repeatedly add a plan to the join order
      while (!tableplanners.isEmpty()) {
         Plan p = getLowestJoinPlan(currentplan);
         if (p != null)
            currentplan = p;
         else  // no applicable join
            currentplan = getLowestProductPlan(currentplan);
      }
      
      // Step 4.  Project on the field names and return
      return new ProjectPlan(currentplan, data.fields());
   }
   
   private Plan getLowestSelectPlan() {
      TablePlanner besttp = null;
     
      for (TablePlanner tp : tableplanners) {    	
         if (besttp == null ||  tp.reductionFactor() > besttp.reductionFactor()) {           
            besttp=tp;
         }
      }      
      tableplanners.remove(besttp);
      return besttp.makeSelectPlan();
   }
   
	private Plan getLowestJoinPlan(Plan current) {
		TablePlanner besttp = null;
		Plan bestplan = null;
		for (TablePlanner tp : tableplanners) {
			if ((besttp == null
					|| (tp.reductionFactor() > besttp.reductionFactor()) && tp
							.joinsWith(current))){
				besttp = tp;
			}
		}
		if (besttp != null) {
			tableplanners.remove(besttp);
			bestplan = besttp.makeJoinPlan(current);
		}
		return bestplan;
	}
	
	
   private Plan getLowestProductPlan(Plan current) {
      TablePlanner besttp = null;
      for (TablePlanner tp : tableplanners) {
         if (besttp == null || tp.reductionFactor()>besttp.reductionFactor()) {
            besttp = tp;
         }
      }
      tableplanners.remove(besttp);
      return besttp.makeProductPlan(current);
   }
}
