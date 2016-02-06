package simpledb.opt;

import simpledb.tx.Transaction;
import simpledb.record.Schema;
import simpledb.query.*;
import simpledb.index.Index;
import simpledb.index.query.*;
import simpledb.metadata.IndexInfo;
import simpledb.multibuffer.MultiBufferProductPlan;
import simpledb.server.SimpleDB;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * This class contains methods for planning a single table.
 * @author Edward Sciore
 */

/**
 * multiple indexes on the same field has been handled. Currently, since the system
 * can only suppport = predicate for selection, both shash and btree indexes can be used. 
 * No further analysis is need. After implemeting >,< predicates in the selection, the optimizer 
 * should be carefull about selecting from mltiple indexes on the same field. 
 * 
 * makeIndexSelect and makeIndexjoin have been changed according to the above explanation.
 * @author mustafautku
 *
 */
class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String,ArrayList<IndexInfo>> indexes;
   private Transaction tx;
   
   /**
    * Creates a new table planner.
    * The specified predicate applies to the entire query.
    * The table planner is responsible for determining
    * which portion of the predicate is useful to the table,
    * and when indexes are useful.
    * @param tblname the name of the table
    * @param mypred the query predicate
    * @param tx the calling transaction
    */
   public TablePlanner(String tblname, Predicate mypred, Transaction tx) {
      this.mypred  = mypred;
      this.tx  = tx;
      myplan   = new TablePlan(tblname, tx);
      myschema = myplan.schema();
      indexes  = SimpleDB.mdMgr().getIndexInfo(tblname, tx);
   }
   
   /**
    * Constructs a select plan for the table.
    * The plan will use an indexselect, if possible.
    * @return a select plan for the table.
    */
   public Plan makeSelectPlan() {
      Plan p = makeIndexSelect();
      if (p == null)
         p = myplan;
      return addSelectPred(p);
   }
   
   /**
    * Constructs a join plan of the specified plan
    * and the table.  The plan will use an indexjoin, if possible.
    * (Which means that if an indexselect is also possible,
    * the indexjoin operator takes precedence.)
    * The method returns null if no join is possible.
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeJoinPlan(Plan current) {
      Schema currsch = current.schema();
      Predicate joinpred = mypred.joinPred(myschema, currsch);
      if (joinpred == null)
         return null;
      Plan p = makeIndexJoin(current, currsch);
      if (p == null)
         p = makeProductJoin(current, currsch);
      return p;
   }
   
   /**
    * Constructs a product plan of the specified plan and
    * this table.
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current) {
      Plan p = addSelectPred(myplan);
      return new MultiBufferProductPlan(current, p, tx);
   }
   
	private Plan makeIndexSelect() {
		for (String fldname : indexes.keySet()) {
			Constant val = mypred.equatesWithConstant(fldname);
			if (val != null) {
				// IndexInfo ii = indexes.get(fldname);
				ArrayList<IndexInfo> ii_list = indexes.get(fldname);
				Iterator<IndexInfo> it = ii_list.iterator();
				while (it.hasNext()) {
					IndexInfo ii = it.next();
					if (ii.getIndexType().equalsIgnoreCase("btree") || ii.getIndexType().equalsIgnoreCase("shash")) {
						return new IndexSelectPlan(myplan, ii, val, tx);
					}
				}
				// return new IndexSelectPlan(myplan, ii, val, tx);
			}
		}
		return null;
	}
   
   private Plan makeIndexJoin(Plan current, Schema currsch) {
      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
//            IndexInfo ii = indexes.get(fldname);
        	 ArrayList<IndexInfo> ii_list = indexes.get(fldname);
				Iterator<IndexInfo> it = ii_list.iterator();
				while (it.hasNext()) {
					IndexInfo ii = it.next();
					if (ii.getIndexType().equalsIgnoreCase("btree") || ii.getIndexType().equalsIgnoreCase("shash")) {
						Plan p =  new IndexJoinPlan(current, myplan, ii, outerfield, tx);
						p = addSelectPred(p);
			            return addJoinPred(p, currsch);
					}
				}
//            Plan p = new IndexJoinPlan(current, myplan, ii, outerfield, tx);
//            p = addSelectPred(p);
//            return addJoinPred(p, currsch);
         }
      }
      return null;
   }
   
   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current);
      return addJoinPred(p, currsch);
   }
   
   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectPred(myschema);
      if (selectpred != null)
         return new SelectPlan(p, selectpred);
      else
         return p;
   }
   
   private Plan addJoinPred(Plan p, Schema currsch) {
      Predicate joinpred = mypred.joinPred(currsch, myschema);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }
}
