package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Plan class for the <i>materialize</i> operator.
 * @author Edward Sciore
 */
public class MaterializePlan implements Plan {
   private Plan srcplan;
   private Transaction tx;
   
   /**
    * Create a materialize plan for the specified query.
    * @param srcplan the plan of the underlying query
    * @param tx the calling transaction
    */
   public MaterializePlan(Transaction tx, Plan srcplan) {
      this.srcplan = srcplan;
      this.tx = tx;
   }
   
   /**
    * This method loops through the underlying query,
    * copying its output records into a temporary table.
    * It then returns a table scan for that table.
    * @see Plan#open()
    */
   public Scan open() {
      Schema sch = srcplan.schema();
      TempTable temp = new TempTable(tx, sch);
      Scan src = srcplan.open();
      UpdateScan dest = temp.open();
      while (src.next()) {
         dest.insert();
         for (String fldname : sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
      }
      src.close();
      dest.beforeFirst();
      return dest;
   }
   
   /**
    * Return the estimated number of blocks in the 
    * materialized table.
    * It does <i>not</i> include the one-time cost
    * of materializing the records.
    * @see Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      // create a dummy Layout object to calculate record length
      Layout layout = new Layout(srcplan.schema());
      double rpb = (double) (tx.blockSize() / layout.slotSize());
      return (int) Math.ceil(srcplan.recordsOutput() / rpb);
   }
   
   /**
    * Return the number of records in the materialized table,
    * which is the same as in the underlying plan.
    * @see Plan#recordsOutput()
    */
   public int recordsOutput() {
      return srcplan.recordsOutput();
   }
   
   /**
    * Return the number of distinct field values,
    * which is the same as in the underlying plan.
    * @see Plan#distinctValues(String)
    */
   public int distinctValues(String fldname) {
      return srcplan.distinctValues(fldname);
   }
   
   /**
    * Return the schema of the materialized table,
    * which is the same as in the underlying plan.
    * @see Plan#schema()
    */
   public Schema schema() {
      return srcplan.schema();
   }

   public String getQueryPlan(String tblname, String currQueryPlan) {
      return "";
   }

   @Override
   public String getQueryPlan(String tblname, String currQueryPlan, int margin) {
	   return ""; 
   }
}
