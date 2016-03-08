package simpledb.query;

/**
 * The class that wraps Java ints as database constants.
 * @author Edward Sciore
 */
public class DoubleConstant implements Constant {
   private Double val;
   
   /**
    * Create a constant by wrapping the specified int.
    * @param n the int value
    */
   public DoubleConstant(double n) {
      val = new Double(n);
   }
   
   /**
    * Unwraps the Integer and returns it.
    * @see simpledb.query.Constant#asJavaVal()
    */
   public Object asJavaVal() {
      return val;
   }
   
   public boolean equals(Object obj) {
      DoubleConstant ic = (DoubleConstant) obj;
      return ic != null && val.equals(ic.val);
   }
   
   public int compareTo(Constant c) {
      DoubleConstant ic = (DoubleConstant) c;
      return val.compareTo(ic.val);
   }
   
   public int hashCode() {
      return val.hashCode();
   }
   
   public String toString() {
      return val.toString();
   }
}
