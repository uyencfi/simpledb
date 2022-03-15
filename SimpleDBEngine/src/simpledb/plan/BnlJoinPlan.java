package simpledb.plan;

import simpledb.materialize.MaterializePlan;
import simpledb.materialize.TempTable;
import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Plan class for the <i>block nested-loop join</i> operator.
 */
public class BnlJoinPlan implements Plan {
    private Transaction tx;
    private Plan lhs, rhs;
    private Predicate joinPred;
    private Schema sch = new Schema();
    private String lField, rField; 

    public BnlJoinPlan(Transaction tx, Plan lhs, Plan rhs, Predicate joinPred, String lField, String rField) {
        this.tx = tx;
        this.lhs = lhs;
        this.rhs = rhs;
        this.joinPred = joinPred;
        this.lField = lField; 
        this.rField = rField; 
        sch.addAll(lhs.schema());
        sch.addAll(rhs.schema());
    }

    @Override
    public Scan open() {
        // Multibuffer: The method open materializes both the left- side and right-side records
        // the left side as a MaterializeScan and the right side as a temporary table.
        Scan rightScan = rhs.open();
        TempTable tt = copyRecordsFrom(lhs);
        return new BnlJoinScan(tx, tt.tableName(), tt.getLayout(), rightScan, joinPred);
    }

    private TempTable copyRecordsFrom(Plan p) {
        Scan   src = p.open();
        Schema sch = p.schema();
        TempTable t = new TempTable(tx, sch);
        UpdateScan dest = (UpdateScan) t.open();
        while (src.next()) {
            dest.insert();
            for (String fldname : sch.fields())
                dest.setVal(fldname, src.getVal(fldname));
        }
        src.close();
        dest.close();
        return t;
    }


    @Override
    public int blocksAccessed() {
        int available = tx.availableBuffs() - 2;
        int leftBlocks = new MaterializePlan(tx, lhs).blocksAccessed();
        int numChunks = (int) Math.ceil((double) leftBlocks / available);
        return leftBlocks + (numChunks * rhs.blocksAccessed());
    }

    // TODO This is the upper bound. Need better estimate
    @Override
    public int recordsOutput() {
    	int maxvals = Math.max(lhs.distinctValues(lField),
                rhs.distinctValues(rField));
        return (lhs.recordsOutput() * rhs.recordsOutput()) / maxvals;
    }

    @Override
    public int distinctValues(String fldname) {
        if (lhs.schema().hasField(fldname)) {
            return lhs.distinctValues(fldname);
        } else {
            return rhs.distinctValues(fldname);
        }
    }

    @Override
    public Schema schema() {
        return sch;
    }
    
    public String getQueryPlan(String tblname, String currQueryPlan, int margin) {
        String padding = " ".repeat(margin);
        return String.format(
                "Bnl join\n" +
                "  cond: %s\n" +
                "  -> %s\n" +
                "  -> %s",
                joinPred, currQueryPlan.replaceAll("\n", "\n" + padding),
                rhs.getQueryPlan(tblname, currQueryPlan, margin + 5).replaceAll("\n", "\n" + padding));
    }
}
