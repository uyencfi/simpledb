package simpledb.query;

import simpledb.parse.*;
import simpledb.plan.Plan;
import simpledb.record.Schema;

/**
 * A term is a comparison between two expressions.
 * @author Edward Sciore
 *
 */
public class Term {
   private Expression lhs, rhs;
   private String opr;
   
   /**
    * Create a new term that compares two expressions
    * for equality.
    * @param lhs  the LHS expression
    * @param rhs  the RHS expression
    */
   public Term(Expression lhs, Expression rhs) {
      this.lhs = lhs;
      this.rhs = rhs;
      this.opr = "=";
   }

   /**
    * Creates a new term that compares two expressions
    * with a custom operator (equal, non-equal, greater/less than,
    * greater/less than or equal).
    * @param lhs the LHS expression
    * @param rhs the RHS expression
    * @param opr the operator
    */
   public Term(Expression lhs, Expression rhs, String opr) {
      this.lhs = lhs;
      this.rhs = rhs;
      this.opr = opr;
   }

   public Expression getLhs() {
      return lhs;
   }

   public Expression getRhs() {
      return rhs;
   }

   /**
    * Return true if both of the term's expressions
    * evaluate to the same constant,
    * with respect to the specified scan.
    * @param s the scan
    * @return true if both expressions have the same value in the scan
    */
   public boolean isSatisfied(Scan s) {
      Constant lhsval = lhs.evaluate(s);
      Constant rhsval = rhs.evaluate(s);
      switch (opr) {
      case "=":
         return lhsval.equals(rhsval);
      case "<>":
      case "!=":
         return !lhsval.equals(rhsval);
      case "<":
         return lhsval.compareTo(rhsval) < 0;
      case ">":
         return lhsval.compareTo(rhsval) > 0;
      case "<=":
         return lhsval.compareTo(rhsval) <= 0;
      case ">=":
         return lhsval.compareTo(rhsval) >= 0;
      default:
         throw new BadSyntaxException();
      }
   }
   
   /**
    * Calculate the extent to which selecting on the term reduces 
    * the number of records output by a query.
    * For example if the reduction factor is 2, then the
    * term cuts the size of the output in half.
    * @param p the query's plan
    * @return the integer reduction factor.
    */
   public int reductionFactor(Plan p) {
      String lhsName, rhsName;
      if (lhs.isFieldName() && rhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         rhsName = rhs.asFieldName();
         return Math.max(p.distinctValues(lhsName),
                         p.distinctValues(rhsName));
      }
      if (lhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         return p.distinctValues(lhsName);
      }
      if (rhs.isFieldName()) {
         rhsName = rhs.asFieldName();
         return p.distinctValues(rhsName);
      }
      // otherwise, the term equates constants
      if (lhs.asConstant().equals(rhs.asConstant()))
         return 1;
      else
         return Integer.MAX_VALUE;
   }
   
   /**
    * Determine if this term is of the form "F=c"
    * where F is the specified field and c is some constant.
    * If so, the method returns that constant.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the constant or null
    */
   public Constant equatesWithConstant(String fldname) {
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          !rhs.isFieldName())
         return rhs.asConstant();
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               !lhs.isFieldName())
         return lhs.asConstant();
      else
         return null;
   }
   
   /**
    * Determine if this term is of the form "F1=F2"
    * where F1 is the specified field and F2 is another field.
    * If so, the method returns the name of that field.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the name of the other field, or null
    */
   public String equatesWithField(String fldname) {
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          rhs.isFieldName())
         return rhs.asFieldName();
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               lhs.isFieldName())
         return lhs.asFieldName();
      else
         return null;
   }
   
   /**
    * Return true if both of the term's expressions
    * apply to the specified schema.
    * @param sch the schema
    * @return true if both expressions apply to the schema
    */
   public boolean appliesTo(Schema sch) {
      return lhs.appliesTo(sch) && rhs.appliesTo(sch);
   }
   
   public String toString() {
      return lhs.toString() + " " + opr + " " + rhs.toString();
   }

   public boolean isNonEqualityTerm() {
      return !opr.equals("=");
   }
}
