package simpledb.materialize;

import java.util.HashSet;

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
   private boolean isDistinct;
   private HashSet<Integer> set = new HashSet<>(); 
   
   /**
    * Create a avg aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public AvgFn(String fldname, boolean isDistinct) {
      this.fldname = fldname;
      this.isDistinct = isDistinct; 
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
	   set.add(sum); 
   }
   
   /**
    * Since SimpleDB does not support null values,
    * this method always increments the count,
    * regardless of the field.
    * @see AggregationFn#processNext(Scan)
    */
   public void processNext(Scan s) {
      int nextVal = s.getVal(fldname).asInt();
	  if (isDistinct && set.contains(nextVal)) {
		  return;
	  }
	  set.add(nextVal); 
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