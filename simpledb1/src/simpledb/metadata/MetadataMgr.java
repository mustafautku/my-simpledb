package simpledb.metadata;

import simpledb.tx.Transaction;
import simpledb.record.*;

import java.util.ArrayList;
import java.util.Map;


/**
 * setStatInfo function is added in order to load more accurate catalog info from above modules. 
 * (or testcases)
 * 
 * dropIndex is added.
 * 
 * getIndexInfo function prtotype is changed to handle different indextypes.
 * 
 * createIndex function return type is changed. Because 2 same type indexes on the same field 
 * are not permitted.
 * @author mustafautku
 *
 */
public class MetadataMgr {
   private static TableMgr  tblmgr;
   private static ViewMgr   viewmgr;
   private static StatMgr   statmgr;
   private static IndexMgr  idxmgr;
   
   public MetadataMgr(boolean isnew, Transaction tx) {
      tblmgr  = new TableMgr(isnew, tx);
      viewmgr = new ViewMgr(isnew, tblmgr, tx);
      statmgr = new StatMgr(tblmgr, tx);
      idxmgr  = new IndexMgr(isnew, tblmgr, tx);
   }
   
   public void createTable(String tblname, Schema sch, Transaction tx) {
      tblmgr.createTable(tblname, sch, tx);
   }
   
   public TableInfo getTableInfo(String tblname, Transaction tx) {
      return tblmgr.getTableInfo(tblname, tx);
   }
   
   public void createView(String viewname, String viewdef, Transaction tx) {
      viewmgr.createView(viewname, viewdef, tx);
   }
   
   public String getViewDef(String viewname, Transaction tx) {
      return viewmgr.getViewDef(viewname, tx);
   }
   
   public /*void*/ int createIndex(String idxname, String tblname, String fldname, String indextype, Transaction tx) {
      return idxmgr.createIndex(idxname, tblname, fldname,indextype, tx);
   }
   public /*void*/ int dropIndex(String idxname, String tblname, String fldname, String indextype, Transaction tx) {   
	   return idxmgr.dropIndex(idxname, tblname, fldname,indextype, tx);
   }
   public Map<String,ArrayList<IndexInfo>> getIndexInfo(String tblname, Transaction tx) {
      return idxmgr.getIndexInfo(tblname, tx);
   }
   
   public StatInfo getStatInfo(String tblname, TableInfo ti, Transaction tx) {
      return statmgr.getStatInfo(tblname, ti, tx);
   }
   
   // Added for setting stat_field distributions manually. 
   public void setStatInfo(String tblname, TableInfo ti,
			Map<String, Integer> fieldstats, Transaction tx) {
	  statmgr.setStatInfo(tblname, ti, fieldstats, tx);
	   }

}
