package simpledb.plan;

import simpledb.materialize.MaterializePlan;
import simpledb.materialize.TempTable;
import simpledb.multibuffer.MultibufferProductScan;
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

    public BnlJoinPlan(Transaction tx, Plan lhs, Plan rhs, Predicate joinPred) {
        this.tx = tx;
        this.lhs = lhs;
        this.rhs = rhs;
        this.joinPred = joinPred;
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
        return lhs.recordsOutput() * rhs.recordsOutput();
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
}