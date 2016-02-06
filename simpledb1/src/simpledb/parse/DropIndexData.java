package simpledb.parse;

/**
 * A new class: The parser for the <i>drop index</i> statement.
 * @author mustafautku
 */
public class DropIndexData {
   private String idxname, tblname, fldname, idxtype;
   
   /**
    * Saves the table and field names of the specified index.
    */
   public DropIndexData(String idxname, String tblname, String fldname, String idxtype) {
      this.idxname = idxname;
      this.tblname = tblname;
      this.fldname = fldname;
      this.idxtype = idxtype;
   }
   
   /**
    * Returns the name of the index.
    * @return the name of the index
    */
   public String indexName() {
      return idxname;
   }
   
   /**
    * Returns the name of the indexed table.
    * @return the name of the indexed table
    */
   public String tableName() {
      return tblname;
   }
   
   /**
    * Returns the name of the indexed field.
    * @return the name of the indexed field
    */
   public String fieldName() {
      return fldname;
   }
   
   public String idxType() {
	      return idxtype;
	   }
}

