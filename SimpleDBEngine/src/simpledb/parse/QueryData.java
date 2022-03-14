package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.query.Predicate;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private List<AggregationFn> aggregateFields;
   private Collection<String> tables;
   private Predicate pred;
   private List<String> groupByFields;
   private HashMap<String, String> sorts;
   private boolean isDistinct; 
   
   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, List<AggregationFn> aggregateFields, Collection<String> tables,
                    Predicate pred, List<String> groupByFields, HashMap<String, String> sorts, boolean isDistinct) {
      this.fields = fields;
      this.aggregateFields = aggregateFields;
      this.tables = tables;
      this.pred = pred;
      this.groupByFields = groupByFields;
      this.sorts = sorts;
      this.isDistinct = isDistinct; 
   }
   
   /**
    * Returns the fields without aggregation mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }

   /**
    * Returns the fields with aggregation mentioned in the select clause.
    * @return a list of field names
    */
   public List<AggregationFn> aggregateFields() {
      return aggregateFields;
   }

   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }
   
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }
   
   /**
    * Returns the boolean that describes whether 
    * output tuples should be distinct
    * @return the boolean value
    */
   public boolean getIsDistinct() {
	   return isDistinct; 
   }

   /**
    * Returns the fields mentioned in the GROUP BY clause.
    * @return a list of field names
    */
   public List<String> groupByFields() {
      return groupByFields;
   }

   /**
    * Returns the field name representation of the aggregated fields
    * e.g. sum(popuplation) -> sumofpopulation
    */
   public List<String> getAggregatedFieldNames() {
      List<String> names = new ArrayList<>();
      for (AggregationFn f : aggregateFields) {
          names.add(f.fieldName());
      }
      return names;
   }

   /**
    * Returns the fields to sort by, and their order of sorting.
    * e.g. {"sname" = "asc", "majorid" = "desc"}
    * @return a map of sorting field name and its corresponding sort order
    */
   public HashMap<String, String> sorts() {
      // System.out.println(sorts);
      return sorts;
   }

   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      return result;
   }
}
