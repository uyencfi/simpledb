package simpledb.opt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import simpledb.materialize.*;
import simpledb.metadata.MetadataMgr;
import simpledb.parse.QueryData;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.plan.ProjectPlan;
import simpledb.plan.QueryPlanner;
import simpledb.tx.Transaction;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 * @author Edward Sciore
 */
public class HeuristicQueryPlanner implements QueryPlanner {
   private Collection<TablePlanner> tableplanners = new ArrayList<>();
   private MetadataMgr mdm;
   private String queryPlan = ""; 
   
   public HeuristicQueryPlanner(MetadataMgr mdm) {
      this.mdm = mdm;
   }
   
   /**
    * Creates an optimized left-deep query plan using the following
    * heuristics.
    * H1. Choose the smallest table (considering selection predicates)
    * to be first in the join order.
    * H2. Add the table to the join order which
    * results in the smallest output.
    */
   public Plan createPlan(QueryData data, Transaction tx) {
      
      // Step 1:  Create a TablePlanner object for each mentioned table
      for (String tblname : data.tables()) {
         TablePlanner tp = new TablePlanner(tblname, data.pred(), tx, mdm);
         tableplanners.add(tp);
      }
      
      // Step 2:  Choose the lowest-size plan to begin the join order
      Plan currentplan = getLowestSelectPlan();
      
      // Step 3:  Repeatedly add a plan to the join order
      while (!tableplanners.isEmpty()) {
         Plan p = getLowestJoinPlan(currentplan);
         if (p != null)
            currentplan = p;
         else  // no applicable join
            currentplan = getLowestProductPlan(currentplan);
      }
      

      // Step 4: Aggregate if present
      Plan p = currentplan;
      if (!data.groupByFields().isEmpty() || !data.aggregateFields().isEmpty()) {
//         System.out.println("group by plan created");
         p = new GroupByPlan(tx, p, data.groupByFields(), data.aggregateFields());
         this.queryPlan = p.getQueryPlan("", this.queryPlan);
      }

      // Step 5. Project on the field names
      List<String> projectNames = data.fields();
      projectNames.addAll(data.getAggregatedFieldNames());
      p = new ProjectPlan(p, projectNames);
      this.queryPlan = p.getQueryPlan("", this.queryPlan);
      
      // If no need to sort or get distinct, just return p.
      if (data.sorts().isEmpty() && !data.getIsDistinct()) {
         System.out.println(this.queryPlan + "\n");
         return p;
      }

      // Else, add a SortPlan node
      HashMap<String, String> sortMap = new HashMap<>(); 
      if (!data.sorts().isEmpty()) {
//         System.out.println("sort plan created");
    	 sortMap = data.sorts();
      } else {
    	 for (String field : p.schema().fields()) {
     		sortMap.put(field, "asc");
    	 }
      }
      // Step 6. Order by and remove duplicates if distinct specified
      p = new SortPlan(tx, p, sortMap, data.getIsDistinct()); 
      this.queryPlan = p.getQueryPlan("", this.queryPlan); 
      
      System.out.println(this.queryPlan + "\n");

      return p;
   }
   
   private Plan getLowestSelectPlan() {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeSelectPlan();
         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
            besttp = tp;
            bestplan = plan;
         }
      }
      this.queryPlan = bestplan.getQueryPlan(besttp.getTblname(), this.queryPlan);
      tableplanners.remove(besttp);
      return bestplan;
   }
   
   private Plan getLowestJoinPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeJoinPlan(current);
         if (plan != null && (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput())) {
            besttp = tp;
            bestplan = plan;
         }
      }
      if (bestplan != null)
    	 this.queryPlan = bestplan.getQueryPlan(besttp.getTblname(), this.queryPlan);
         tableplanners.remove(besttp);
      return bestplan;
   }
   
   private Plan getLowestProductPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeProductPlan(current);
         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
            besttp = tp;
            bestplan = plan;
         }
      }
      this.queryPlan = bestplan.getQueryPlan(besttp.getTblname(), this.queryPlan);
      tableplanners.remove(besttp);
      return bestplan;
   }

   public void setPlanner(Planner p) {
      // for use in planning views, which
      // for simplicity this code doesn't do.
   }
}
