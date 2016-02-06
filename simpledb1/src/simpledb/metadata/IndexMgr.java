package simpledb.metadata;

import static simpledb.metadata.TableMgr.MAX_NAME;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.record.*;

import java.io.File;
import java.util.*;

/**
 * The index manager.
 * The index manager has similar functionalty to the table manager.
 * @author Edward Sciore
 */

/**
 * All functions in the class have been changed to handle catalog organization of 
 * different type of indexes. Indextype arguman has been added to the functions.
 * @author mustafautku
 *
 */
public class IndexMgr {
   private TableInfo ti;
   
   /**
    * Creates the index manager.
    * This constructor is called during system startup.
    * If the database is new, then the <i>idxcat</i> table is created.
    * @param isnew indicates whether this is a new database
    * @param tx the system startup transaction
    */
   public IndexMgr(boolean isnew, TableMgr tblmgr, Transaction tx) {
      if (isnew) {
         Schema sch = new Schema();
         sch.addStringField("indexname", MAX_NAME);
         sch.addStringField("tablename", MAX_NAME);
         sch.addStringField("fieldname", MAX_NAME);
         sch.addStringField("indextype", MAX_NAME);
         tblmgr.createTable("idxcat", sch, tx);
      }
      ti = tblmgr.getTableInfo("idxcat", tx);
   }
   
   /**
    * Creates an index of the specified type for the specified field.
    * A unique ID is assigned to this index, and its information
    * is stored in the idxcat table.
    * @param idxname the name of the index
    * @param tblname the name of the indexed table
    * @param fldname the name of the indexed field
    * @param tx the calling transaction
    */
   public /*void*/int createIndex(String idxname, String tblname, String fldname, String indextype,Transaction tx) {
      RecordFile rf = new RecordFile(ti, tx);
      //first check if the same type idx exist on the fldname already.
      rf.beforeFirst();
      while(rf.next()){
    	  if(rf.getString("fieldname").equalsIgnoreCase(fldname) && rf.getString("indextype").equalsIgnoreCase(indextype)){
    		  rf.close();
    		  return -1;
    	  }    		  
      }
      rf.beforeFirst();
      rf.insert();
      rf.setString("indexname", idxname);
      rf.setString("tablename", tblname);
      rf.setString("fieldname", fldname);
      rf.setString("indextype", indextype);
      rf.close();
//      rf.insert();
//      rf.setString("indexname", idxname);
//      rf.setString("tablename", tblname);
//      rf.setString("fieldname", fldname);
//      rf.setString("indextype", indextype);
//      rf.close();
      return 1;
   }
   /**
    * Deletes the index of the specified type for the specified field.
    * Delete indx info from idxcat table and from file system directory.
    * @param idxname the name of the index
    * @param tblname the name of the indexed table
    * @param fldname the name of the indexed field
    * @param tx the calling transaction
    */
	public int dropIndex(String idxname, String tblname, String fldname,
			String indextype, Transaction tx) {
		RecordFile rf = new RecordFile(ti, tx);
		int success = 0;
		if (idxname != null) {
			while (rf.next())
				if (rf.getString("indexname").equalsIgnoreCase(idxname)
						&& rf.getString("tablename").equals(tblname)) {
					rf.delete();
					success++;
					// break
				}
		} else {
			while (rf.next())
				if (rf.getString("tablename").equals(tblname)) {
					rf.delete();
					success++;
				}
		}
		rf.close();
		if (idxname == null)//FOR DROP INDEXALL, manually delete idx files.
			return success;
		// String homedir = System.getProperty("user.home");
		File dbDirectory = SimpleDB.fileMgr().getDBdirectory();
		if (!dbDirectory.exists())
			System.err.println("fatal errror!:");
		
		Map<String, ArrayList<IndexInfo>> indexes = getIndexInfo(tblname, tx);
		ArrayList<IndexInfo> ii_list = indexes.get(fldname);
		if (ii_list != null) {
			Iterator<IndexInfo> it = ii_list.iterator();
			while (it.hasNext()) {
				IndexInfo ii = it.next();
				if(ii.indextype.equalsIgnoreCase(indextype)){
					Index idx = ii.open();
					idx.close();
				}
			}
		}
		
		// remove a specific index table : Not guaranteed. Be careful..
		for (String filename : dbDirectory.list())
			if (filename.startsWith(idxname)) {
				new File(dbDirectory, filename).delete();
			}

		return success;
	}
   /**
    * Returns a map containing the index info for all indexes
    * on the specified table.
    * @param tblname the name of the table
    * @param tx the calling transaction
    * @return a map of IndexInfo objects, keyed by their field names
    */
   public Map<String,ArrayList<IndexInfo>> getIndexInfo(String tblname, Transaction tx) {
		Map<String, ArrayList<IndexInfo>> result = new HashMap<String, ArrayList<IndexInfo>>();
		RecordFile rf = new RecordFile(ti, tx);
		while (rf.next())
			if (rf.getString("tablename").equals(tblname)) {
				String idxname = rf.getString("indexname");
				String fldname = rf.getString("fieldname");
				String indextype = rf.getString("indextype");
				IndexInfo ii = new IndexInfo(idxname, tblname, fldname,
						indextype, tx);
				if (result.containsKey(fldname)) {
					result.get(fldname).add(ii);
				}
				else{
					ArrayList<IndexInfo> indexesOnField = new ArrayList<IndexInfo>();
					indexesOnField.add(ii);
					result.put(fldname, indexesOnField);
				}
			}
		rf.close();
		return result;
   }
}
