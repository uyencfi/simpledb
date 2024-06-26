package simpledb.materialize;

import java.util.*;

import simpledb.plan.Plan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Plan class for the <i>sort</i> operator.
 * @author Edward Sciore
 */
public class SortPlan implements Plan {
   private Transaction tx;
   private Plan p;
   private Schema sch;
   private RecordComparator comp;
   private boolean isDistinct;
   private HashMap<String, String> sortfields;
   
   /**
    * Create a sort plan for the specified query.
    * @param p the plan for the underlying query
    * @param sortfields the fields to sort by
    * @param tx the calling transaction
    */
   public SortPlan(Transaction tx, Plan p, HashMap<String, String> sortfields, boolean isDistinct) {
      this.tx = tx;
      this.p = p;
      this.isDistinct = isDistinct; 
      sch = p.schema();
      comp = new RecordComparator(sortfields);
      this.sortfields = sortfields;
   }
   
   /**
    * This method is where most of the action is.
    * Up to 2 sorted temporary tables are created,
    * and are passed into SortScan for final merging.
    * @see Plan#open()
    */
   public Scan open() {
      Scan src = p.open();
      List<TempTable> runs = splitIntoRuns(src);
      src.close();
      while (runs.size() > 1)
         runs = doAMergeIteration(runs);
      	 
      return new SortScan(runs, comp);
   }
   
   /**
    * Return the number of blocks in the sorted table,
    * which is the same as it would be in a
    * materialized table.
    * It does <i>not</i> include the one-time cost
    * of materializing and sorting the records.
    * @see Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      Plan mp = new MaterializePlan(tx, p); // not opened; just for analysis
      // calculate cost for sorting 
      double numPartitions = Math.ceil((double) mp.blocksAccessed() / tx.availableBuffs());
 
      
      double x = Math.ceil(numPartitions / tx.availableBuffs()); 
      double numMergePasses = Math.ceil(Math.log(x) / Math.log((double) tx.availableBuffs() - 1));
      int result = (int) (mp.blocksAccessed() * (1 + numMergePasses));
      
      return result;
   }
   
   /**
    * Return the number of records in the sorted table,
    * which is the same as in the underlying query.
    * @see Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }
   
   /**
    * Return the number of distinct field values in
    * the sorted table, which is the same as in
    * the underlying query.
    * @see Plan#distinctValues(String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }
   
   /**
    * Return the schema of the sorted table, which
    * is the same as in the underlying query.
    * @see Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
   
   private List<TempTable> splitIntoRuns(Scan src) {
      List<TempTable> temps = new ArrayList<>();
      src.beforeFirst();
      if (!src.next())
         return temps;
      TempTable currenttemp = new TempTable(tx, sch);
      temps.add(currenttemp);
      UpdateScan currentscan = currenttemp.open();
      while (copy(src, currentscan))
         if (comp.compare(src, currentscan) < 0) {
         // start a new run
         currentscan.close();
         currenttemp = new TempTable(tx, sch);
         temps.add(currenttemp);
         currentscan = (UpdateScan) currenttemp.open();
      }
      currentscan.close();
      return temps;
   }
   
   private List<TempTable> doAMergeIteration(List<TempTable> runs) {
      List<TempTable> result = new ArrayList<>();
      while (runs.size() > 1) {
         TempTable p1 = runs.remove(0);
         TempTable p2 = runs.remove(0);
         result.add(mergeTwoRuns(p1, p2));
      }
      if (runs.size() == 1)
         result.add(runs.get(0));
      return result;
   }
   
   private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
      Scan src1 = p1.open();
      Scan src2 = p2.open();
      TempTable result = new TempTable(tx, sch);
      UpdateScan dest = result.open();
      
      boolean hasmore1 = src1.next();
      boolean hasmore2 = src2.next();
      
      while (hasmore1 && hasmore2) {
    	 int compareResult = comp.compare(src1,  src2);
	     if (compareResult < 0)
	        hasmore1 = copy(src1, dest);
	     else if (compareResult == 0 && isDistinct)
	    	hasmore1 = src1.next();
	     else
	        hasmore2 = copy(src2, dest);
      }
      
      if (hasmore1)
         while (hasmore1)
         hasmore1 = copy(src1, dest);
      else
         while (hasmore2)
         hasmore2 = copy(src2, dest);
      src1.close();
      src2.close();
      dest.close();
      return result;
   }
   
   private boolean copy(Scan src, UpdateScan dest) {
	  HashMap<String, Constant> store = new HashMap<>();
      dest.insert();
      
      for (String fldname : sch.fields()) {
         dest.setVal(fldname, src.getVal(fldname));
      	 store.put(fldname, src.getVal(fldname));
      }
      
      boolean next = src.next();
      if (isDistinct) {
	      while (next) {
	    	 for (String fldname : sch.fields()) {
	             if (!store.get(fldname).equals(src.getVal(fldname))) {
	            	 return next;
	             }
	          }
	    	 next = src.next();
	      }
      }
      
      return next;
   }
   
   public String getQueryPlan(String tblname, String currQueryPlan) {
	   String sortBy = "sort by";
	   for (Map.Entry<String, String> element: sortfields.entrySet()) {
		   sortBy += String.format(" %s %s,", element.getKey(), element.getValue());
	   }
	   sortBy = sortBy.substring(0, sortBy.length() - 1);
	   return String.format("%s \n %s%s", currQueryPlan, isDistinct ? "get distinct and " : "", sortBy);
   }

   @Override
   public String getQueryPlan(String tblname, String currQueryPlan, int margin) {
       String padding = " ".repeat(margin);
       StringBuilder sortBy = (isDistinct)
               ? new StringBuilder("Get distinct and Sort by")
               : new StringBuilder("Sort by");
       for (Map.Entry<String, String> element: sortfields.entrySet()) {
           sortBy.append(String.format(" %s %s,", element.getKey(), element.getValue()));
       }
       return String.format(
               "%s\n" +
               "  -> %s",
               sortBy.substring(0, sortBy.length() - 1), currQueryPlan.replaceAll("\n", "\n" + padding));
   }
}
