package simpledb.materialize;

import java.util.HashSet;

import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The <i>count</i> aggregation function.
 * @author Edward Sciore
 */
public class CountFn implements AggregationFn {
   private String fldname;
   private int count;
   private HashSet<Constant> set = new HashSet<>(); 
   private boolean isDistinct; 
   
   /**
    * Create a count aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public CountFn(String fldname, boolean isDistinct) {
      this.fldname = fldname;
      this.isDistinct = isDistinct; 
   }
   
   /**
    * Start a new count.
    * Since SimpleDB does not support null values,
    * every record will be counted,
    * regardless of the field.
    * The current count is thus set to 1.
    * @see AggregationFn#processFirst(Scan)
    */
   public void processFirst(Scan s) {
      count = 1;
      set.add(s.getVal(fldname)); 
   }
   
   /**
    * Since SimpleDB does not support null values,
    * this method always increments the count,
    * regardless of the field.
    * @see AggregationFn#processNext(Scan)
    */
   public void processNext(Scan s) {
      Constant nextVal = s.getVal(fldname);
	  if (isDistinct && set.contains(nextVal)) {
		  return;
	  }
	  set.add(nextVal); 
	  count++;
   }
   
   /**
    * Return the field's name, prepended by "countof".
    * @see AggregationFn#fieldName()
    */
   public String fieldName() {
      return "countof" + fldname;
   }
   
   /**
    * Return the current count.
    * @see AggregationFn#value()
    */
   public Constant value() {
      return new Constant(count);
   }
   
   public String getQueryPlan() {
	   return String.format("count(%s%s)", isDistinct ? "distinct" : "",fldname);
   }
}
