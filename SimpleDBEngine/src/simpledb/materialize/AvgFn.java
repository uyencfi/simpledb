package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The <i>avg</i> aggregation function.
 * @author Edward Sciore
 */
public class AvgFn implements AggregationFn {
   private String fldname;
   private int sum;
   private int count; 
   
   /**
    * Create a avg aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public AvgFn(String fldname) {
      this.fldname = fldname;
   }
   
   /**
    * Start a new avg.
    * Since SimpleDB does not support null values,
    * every record will be counted,
    * regardless of the field.
    * The current count is thus set to 1.
    * @see AggregationFn#processFirst(Scan)
    */
   public void processFirst(Scan s) {
	   sum = s.getVal(fldname).asInt();
	   count = 1; 
   }
   
   /**
    * Since SimpleDB does not support null values,
    * this method always increments the count,
    * regardless of the field.
    * @see AggregationFn#processNext(Scan)
    */
   public void processNext(Scan s) {
	  int nextVal = s.getVal(fldname).asInt();
      sum += nextVal;
      count++; 
   }
   
   /**
    * Return the field's name, prepended by "avgof".
    * @see AggregationFn#fieldName()
    */
   public String fieldName() {
      return "avgof" + fldname;
   }
   
   /**
    * Return the current avg.
    * @see AggregationFn#value()
    */
   public Constant value() {
      return new Constant(sum / count);
   }
}