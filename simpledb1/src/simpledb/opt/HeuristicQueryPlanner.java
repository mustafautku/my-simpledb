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
      Plan bestplan = null;
      int bestReductionFactor=1;
      for (TablePlanner tp : tableplanners) {
    	 int currRedFactor=tp.reductionFactor();
         if (bestplan == null ||  currRedFactor> bestReductionFactor) {
            besttp = tp;
            bestReductionFactor=currRedFactor;
            bestplan = tp.makeSelectPlan();
         }
      }
      tableplanners.remove(besttp);
      return bestplan;
   }
   
	private Plan getLowestJoinPlan(Plan current) {
		TablePlanner besttp = null;
		Plan bestplan = null;
		int bestReductionFactor = 1;
		for (TablePlanner tp : tableplanners) {
			Plan candidatePlan = tp.makeJoinPlan(current);
			if (candidatePlan != null) {
				int currRedFactor = tp.reductionFactor(current, candidatePlan); // current=currentPlan;
																				// candidatePlan=Join(current,tp.plan) 
				if (bestplan == null || currRedFactor > bestReductionFactor) {

					besttp = tp;
					bestplan = candidatePlan;
					bestReductionFactor = currRedFactor;
				}
			}
		}
		if (bestplan != null)
			tableplanners.remove(besttp);
		return bestplan;
	}
	
	
   private Plan getLowestProductPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeProductPlan(current);
         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
            besttp = tp;
            bestplan = plan;
         }
      }
      tableplanners.remove(besttp);
      return bestplan;
   }
}
