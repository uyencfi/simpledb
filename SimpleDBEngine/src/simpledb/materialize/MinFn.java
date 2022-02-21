package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The <i>min</i> aggregation function.
 * @author Edward Sciore
 */
public class MinFn implements AggregationFn {
   private String fldname;
   private Constant val;
   
   /**
    * Create a min aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public MinFn(String fldname) {
      this.fldname = fldname;
   }
   
   /**
    * Start a new min.
    * Since SimpleDB does not support null values,
    * every record will be counted,
    * regardless of the field.
    * The current count is thus set to 1.
    * @see AggregationFn#processFirst(Scan)
    */
   public void processFirst(Scan s) {
	   val = s.getVal(fldname);
   }
   
   /**
    * Since SimpleDB does not support null values,
    * this method always increments the count,
    * regardless of the field.
    * @see AggregationFn#processNext(Scan)
    */
   public void processNext(Scan s) {
	   Constant newval = s.getVal(fldname);
	   if (newval.compareTo(val) < 0)
		   val = newval;
   }
   
   /**
    * Return the field's name, prepended by "minof".
    * @see AggregationFn#fieldName()
    */
   public String fieldName() {
      return "minof" + fldname;
   }
   
   /**
    * Return the current min.
    * @see AggregationFn#value()
    */
   public Constant value() {
      return val;
   }
}