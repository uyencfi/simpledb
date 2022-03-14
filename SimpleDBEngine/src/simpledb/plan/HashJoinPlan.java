package simpledb.plan;

import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/** The Plan class corresponding to the <i>hash-join</i>
 * relational algebra operator.
 * (based on simpledb.plan.ProductPlan.java)
 */
public class HashJoinPlan implements Plan {
    private Transaction tx;
    private Plan left, right;
    private String lFieldName, rFieldName;
    private Schema schema = new Schema();

    // // k is the number of partitions
    // private int k;

    /**
     * Creates a new hash node in the query tree,
     * having the two specified subqueries and the
     * join-fields.
     * @param left the left-hand subquery
     * @param right the right-hand subquery
     * @param fldnameLeft the left-hand join field
     * @param fldnameRight the right-hand join field
     */
    public HashJoinPlan(Transaction tx, Plan left, Plan right, String fldnameLeft, String fldnameRight) {
        int numPartitions = tx.availableBuffs() - 1;
        if (numPartitions < 2) {
            // We can not work with just 1 partition
            throw new RuntimeException("not enough buffer");
        }
        this.tx = tx;
        this.left = left;
        this.right = right;
        this.lFieldName = fldnameLeft;
        this.rFieldName = fldnameRight;
        schema.addAll(left.schema());
        schema.addAll(right.schema());
    }

    /**
     * Creates a hash-join scan for this query.
     * @see simpledb.plan.Plan#open()
     */
    public Scan open() {
        Scan lScan = left.open();
        Scan rScan = right.open();
        Schema lsch = left.schema();
        Schema rsch = right.schema();
        return new MyHashScan(tx, lScan, rScan, lsch, rsch, lFieldName, rFieldName);
    }

    /**
     * Estimates the number of block accesses in the hash-join.
     * TODO remove hardcode 3
     * The estimated formula is (no deduplication):
     * <pre> 3 * (|R| + |S|) </pre>
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return 3 * (left.blocksAccessed() + right.blocksAccessed());
    }

    /**
     * Return the number of records in the join.
     * Assuming uniform distribution, the formula is:
     * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
     * @see Plan#recordsOutput()
     */
    public int recordsOutput() {
        int maxvals = Math.max(left.distinctValues(lFieldName),
                right.distinctValues(rFieldName));
        return (left.recordsOutput() * right.recordsOutput()) / maxvals;
    }

    /**
     * Estimates the number of distinct values for the
     * specified field.
     * @see Plan#distinctValues(String)
     */
    public int distinctValues(String fldname) {
        if (left.schema().hasField(fldname))
            return left.distinctValues(fldname);
        else
            return right.distinctValues(fldname);
    }

    /**
     * Returns the schema of the hash-join, which is the union of the
     * schemas of the underlying queries.
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return schema;
    }
}
