package simpledb.opt;

import java.util.*;

import simpledb.index.planner.IndexJoinPlan;
import simpledb.index.planner.IndexSelectPlan;
import simpledb.materialize.*;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.BnlJoinPlan;
import simpledb.plan.HashJoinPlan;
import simpledb.plan.Plan;
import simpledb.plan.SelectPlan;
import simpledb.plan.TablePlan;
import simpledb.query.*;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * This class contains methods for planning a single table.
 * @author Edward Sciore
 */
class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String,IndexInfo> indexes;
   private Transaction tx;
   private String tblname;

   /**
    * Creates a new table planner.
    * The specified predicate applies to the entire query.
    * The table planner is responsible for determining
    * which portion of the predicate is useful to the table,
    * and when indexes are useful.
    * @param tblname the name of the table
    * @param mypred the query predicate
    * @param tx the calling transaction
    */
   public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
      this.mypred  = mypred;
      this.tx  = tx;
      myplan   = new TablePlan(tx, tblname, mdm);
      myschema = myplan.schema();
      indexes  = mdm.getIndexInfo(tblname, tx);
      this.tblname = tblname;
   }
   
   /**
    * Constructs a select plan for the table.
    * The plan will use an indexselect, if possible.
    * @return a select plan for the table.
    */
   public Plan makeSelectPlan() {
      Plan p = makeIndexSelect();
      if (p == null)
         p = myplan;
      return addSelectPred(p);
   }
   
   /**
    * Constructs a join plan of the specified plan
    * and the table.  The plan will choose among an indexjoin (if possible)
    * a sortmerge join, and a product (block nested loop) join.
    * The method returns null if no join is possible.
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeJoinPlan(Plan current) {
      Schema currsch = current.schema();
      Predicate joinpred = mypred.joinSubPred(myschema, currsch);
      if (joinpred == null) {
         return null;
      }

//      System.out.println("Has Non-equality pred: " + joinpred.hasNonEqualityPredicate());
      if (joinpred.hasNonEqualityPredicate()) {
         return makeBlockNestedLoopJoin(current, currsch);
      }

      Plan p;
      Plan bnl = makeBlockNestedLoopJoin(current, currsch);
      Plan index = makeIndexJoin(current, currsch);
      Plan sortMerge = makeMergeJoin(current, currsch);
      Plan hash = makeHashJoin(current, currsch);

      System.out.println("Bnl: " + bnl.recordsOutput());
      if (index != null) System.out.println("index: " + index.recordsOutput());
      System.out.println("sort-merge: " + sortMerge.recordsOutput());
      System.out.println("hash join: " + hash.recordsOutput());

      System.out.println("blocks accessed"); 
      if (index != null) System.out.println("index: " + index.blocksAccessed());
      System.out.println("sort-merge: " + sortMerge.blocksAccessed());
      System.out.println("hash join: " + hash.blocksAccessed());
      
      p = bnl;
      if (index != null && p.blocksAccessed() > index.blocksAccessed()) {
         p = index;
      }
      if (p.blocksAccessed() > hash.blocksAccessed()) {
         p = hash;
      }
      if (p.blocksAccessed() > sortMerge.blocksAccessed()) {
         p = sortMerge;
      }
      // p = bnl;
      return p;
   }

   
   /**
    * Constructs a product plan of the specified plan and
    * this table.
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current) {
      Plan p = addSelectPred(myplan);
      return new MultibufferProductPlan(tx, current, p);
   }

   public Plan makeBlockNestedLoopJoin(Plan current, Schema currsch) {
	  Predicate subPred = mypred.joinSubPred(currsch, myschema);
	  String[] fields = getFields(subPred, currsch);
      return new BnlJoinPlan(tx, current, myplan, subPred, fields[0], fields[1]);
   }

   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
         Constant val = mypred.equatesWithConstant(fldname);
         if (val != null) {
            IndexInfo ii = indexes.get(fldname);
            System.out.println("*index on " + fldname + " used*");
            return new IndexSelectPlan(myplan, ii, val);
         }
      }
      return null;
   }
   
   private Plan makeIndexJoin(Plan current, Schema currsch) {
      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            IndexInfo ii = indexes.get(fldname);
            Plan p = new IndexJoinPlan(current, addSelectPred(myplan), ii, outerfield);
            return p;
            // p = addSelectPred(p);
            // return addJoinPred(p, currsch);
         }
      }
      return null;
   }
   
   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current);
      return addJoinPred(p, currsch);
   }

   private Plan makeMergeJoin(Plan current, Schema currsch) {
      Predicate subPred = mypred.joinSubPred(currsch, myschema);
      String[] fields = getFields(subPred, currsch);
      // System.out.println(Arrays.toString(fields));
      Plan p = new MergeJoinPlan(tx, current, addSelectPred(myplan), fields[0], fields[1]);
      return p;
      // return addJoinPred(p, currsch);
   }

   // for extracting the correct field name in the join predicate;
   // used in makeMergeJoin() and makeHashJoin()
   private String[] getFields(Predicate subPred, Schema currSchema) {
      Term t = subPred.getFirst();
      if (t.getLhs().appliesTo(currSchema)) {
         return new String[] {t.getLhs().asFieldName(), t.getRhs().asFieldName()};
      }
      return new String[] {t.getRhs().asFieldName(), t.getLhs().asFieldName()};
   }

   private Plan makeHashJoin(Plan current, Schema currsch) {
      Predicate subPred = mypred.joinSubPred(currsch, myschema);
      String[] fields = getFields(subPred, currsch);
      // System.out.println(Arrays.toString(fields));
      Plan p = new HashJoinPlan(tx, current, addSelectPred(myplan), fields[0], fields[1]);
      return p;
      // return addJoinPred(p, currsch);
   }


   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectSubPred(myschema);
      if (selectpred != null)
         return new SelectPlan(p, selectpred);
      else
         return p;
   }
   
   private Plan addJoinPred(Plan p, Schema currsch) {
      Predicate joinpred = mypred.joinSubPred(currsch, myschema);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }

   public String getTblname() {
	   return this.tblname;
   }
}
