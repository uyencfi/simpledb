package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.materialize.AvgFn;
import simpledb.materialize.CountFn;
import simpledb.materialize.MaxFn;
import simpledb.materialize.MinFn;
import simpledb.materialize.SumFn;
import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Predicate;
import simpledb.query.Term;
import simpledb.record.Schema;

/**
 * The SimpleDB parser.
 * @author Edward Sciore
 */
public class Parser {
   private Lexer lex;
   
   public Parser(String s) {
      lex = new Lexer(s);
   }
   
// Methods for parsing predicates, terms, expressions, constants, and fields
   
   public String field() {
      return lex.eatId();
   }
   
   public Constant constant() {
      if (lex.matchStringConstant())
         return new Constant(lex.eatStringConstant());
      else
         return new Constant(lex.eatIntConstant());
   }
   
   public Expression expression() {
      if (lex.matchId())
         return new Expression(field());
      else
         return new Expression(constant());
   }
   
   public Term term() {
      Expression lhs = expression();
      // lex.eatDelim('=');
      String opr = lex.eatOpr();
      Expression rhs = expression();
      return new Term(lhs, rhs, opr);
   }
   
   public Predicate predicate() {
      Predicate pred = new Predicate(term());
      if (lex.matchKeyword("and")) {
         lex.eatKeyword("and");
         pred.conjoinWith(predicate());
      }
      return pred;
   }
   
// Methods for parsing queries
   
   public QueryData query() {
      lex.eatKeyword("select");
      
      // distinct
      boolean isDistinct = false; 
      if (lex.matchKeyword("distinct")) {
    	  lex.eatKeyword("distinct");
    	  isDistinct = true; 
      }
      
      List<String> fields = new ArrayList<>();
      List<AggregationFn> aggregateFields = new ArrayList<>();
      selectList(fields, aggregateFields);

      lex.eatKeyword("from");
      Collection<String> tables = tableList();
      Predicate pred = new Predicate();
      HashMap<String, String> sorts = new HashMap<>();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      List<String> groupBy = new ArrayList<>();
      if (lex.matchKeyword("group")) {
         lex.eatKeyword("group");
         lex.eatKeyword("by");
         groupBy = groupByList();
         this.checkSelectInGroupBy(fields, groupBy);
         // System.out.println(groupBy);
      }
      if (lex.matchKeyword("order")) {
         lex.eatKeyword("order");
         lex.eatKeyword("by");
         sorts = sortList();
         // System.out.println(sorts);
      }
      return new QueryData(fields, aggregateFields, tables, pred, groupBy, sorts, isDistinct);
   }

   private void selectList(List<String> fields, List<AggregationFn> aggregateFields) {
      boolean isDistinct = false; 
      if (lex.matchKeyword("sum")) {
         lex.eatKeyword("sum");
         lex.eatDelim('(');
         
         if (lex.matchKeyword("distinct")) {
        	lex.eatKeyword("distinct");
       	  	isDistinct = true; 
         }
         
         aggregateFields.add(new SumFn(field(), isDistinct));
         lex.eatDelim(')');
      } else if (lex.matchKeyword("count")) {
         lex.eatKeyword("count");
         lex.eatDelim('(');
         
         if (lex.matchKeyword("distinct")) {
         	lex.eatKeyword("distinct");
        	  	isDistinct = true; 
         }
         
         aggregateFields.add(new CountFn(field(), isDistinct));
         lex.eatDelim(')');
      } else if (lex.matchKeyword("avg")) {
         lex.eatKeyword("avg");
         lex.eatDelim('(');
         
         if (lex.matchKeyword("distinct")) {
         	lex.eatKeyword("distinct");
        	  	isDistinct = true; 
         }
         
         aggregateFields.add(new AvgFn(field(), isDistinct));
         lex.eatDelim(')');
      } else if (lex.matchKeyword("min")) {
         lex.eatKeyword("min");
         lex.eatDelim('(');
         
         if (lex.matchKeyword("distinct")) {
         	lex.eatKeyword("distinct");
         }
         
         aggregateFields.add(new MinFn(field()));
         lex.eatDelim(')');
      } else if (lex.matchKeyword("max")) {
         lex.eatKeyword("max");
         lex.eatDelim('(');
         
         if (lex.matchKeyword("distinct")) {
         	lex.eatKeyword("distinct"); 
         }
         
         aggregateFields.add(new MaxFn(field()));
         lex.eatDelim(')');
      } else {
         fields.add(field());
      }
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         selectList(fields, aggregateFields);
      }
   }

   private List<String> groupByList() {
      List<String> L = new ArrayList<>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(groupByList());
      }
      return L;
   }

   /**
    * Can only select fields that also appear in GROUP BY clause.
    * Assuming non-empty Group by, throws BadSyntaxException
    * if a select field does not exist in Group by.
    */
   private void checkSelectInGroupBy(List<String> selectFields, List<String> groupByFields) {
      for (String f : selectFields) {
         if (!groupByFields.contains(f)) {
            throw new BadSyntaxException();
         }
      }
   }
   
   private Collection<String> tableList() {
      Collection<String> L = new ArrayList<String>();
      L.add(lex.eatId());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(tableList());
      }
      return L;
   }

   /**
    * Parses the list of fields to sort by.
    * Returns a HashMap<String, String> mapping each sorting field to its sorting order
    * e.g. {"sname" = "asc", "majorid" = "desc"}
    */
   private HashMap<String, String> sortList() {
      HashMap<String, String> map = new LinkedHashMap<>();
      map.put(lex.eatId(), lex.matchSorts() ? lex.eatSorts() : "asc");
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         map.putAll(sortList());
      }
      return map;
   }


// Methods for parsing the various update commands
   
   public Object updateCmd() {
      if (lex.matchKeyword("insert"))
         return insert();
      else if (lex.matchKeyword("delete"))
         return delete();
      else if (lex.matchKeyword("update"))
         return modify();
      else
         return create();
   }
   
   private Object create() {
      lex.eatKeyword("create");
      if (lex.matchKeyword("table"))
         return createTable();
      else if (lex.matchKeyword("view"))
         return createView();
      else
         return createIndex();
   }
   
// Method for parsing delete commands
   
   public DeleteData delete() {
      lex.eatKeyword("delete");
      lex.eatKeyword("from");
      String tblname = lex.eatId();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new DeleteData(tblname, pred);
   }
   
// Methods for parsing insert commands
   
   public InsertData insert() {
      lex.eatKeyword("insert");
      lex.eatKeyword("into");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      List<String> flds = fieldList();
      lex.eatDelim(')');
      lex.eatKeyword("values");
      lex.eatDelim('(');
      List<Constant> vals = constList();
      lex.eatDelim(')');
      return new InsertData(tblname, flds, vals);
   }
   
   private List<String> fieldList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(fieldList());
      }
      return L;
   }
   
   private List<Constant> constList() {
      List<Constant> L = new ArrayList<Constant>();
      L.add(constant());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(constList());
      }
      return L;
   }
   
// Method for parsing modify commands
   
   public ModifyData modify() {
      lex.eatKeyword("update");
      String tblname = lex.eatId();
      lex.eatKeyword("set");
      String fldname = field();
      lex.eatDelim('=');
      Expression newval = expression();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new ModifyData(tblname, fldname, newval, pred);
   }
   
// Method for parsing create table commands
   
   public CreateTableData createTable() {
      lex.eatKeyword("table");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      Schema sch = fieldDefs();
      lex.eatDelim(')');
      return new CreateTableData(tblname, sch);
   }
   
   private Schema fieldDefs() {
      Schema schema = fieldDef();
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         Schema schema2 = fieldDefs();
         schema.addAll(schema2);
      }
      return schema;
   }
   
   private Schema fieldDef() {
      String fldname = field();
      return fieldType(fldname);
   }
   
   private Schema fieldType(String fldname) {
      Schema schema = new Schema();
      if (lex.matchKeyword("int")) {
         lex.eatKeyword("int");
         schema.addIntField(fldname);
      }
      else {
         lex.eatKeyword("varchar");
         lex.eatDelim('(');
         int strLen = lex.eatIntConstant();
         lex.eatDelim(')');
         schema.addStringField(fldname, strLen);
      }
      return schema;
   }
   
// Method for parsing create view commands
   
   public CreateViewData createView() {
      lex.eatKeyword("view");
      String viewname = lex.eatId();
      lex.eatKeyword("as");
      QueryData qd = query();
      return new CreateViewData(viewname, qd);
   }
   
   
//  Method for parsing create index commands
   
   public CreateIndexData createIndex() {
      lex.eatKeyword("index");
      String idxname = lex.eatId();
      lex.eatKeyword("on");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      String fldname = field();
      lex.eatDelim(')');

      if (lex.matchKeyword("using")) {
         lex.eatKeyword("using");
         String idxtype = lex.eatIndex();
         return new CreateIndexData(idxname, tblname, fldname, idxtype);
      } else {
         return new CreateIndexData(idxname, tblname, fldname, "hash");
      }
   }
}

