package simpledb.plan;

import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;

/**
 * The Plan class corresponding to the <i>hash-join</i>
 * relational algebra operator.
 */
public class HashPlan implements Plan {
    private Plan p1, p2;
    private Schema schema = new Schema();
    private String[] fields = new String[2];
    private int tupleSize = (int)Math.floor(SimpleDB.BUFFER_SIZE/1.3);
    // Using a 6-tuple â†‘ for hash-joining (since buffer-size is 8)
    // This value can be modified to fit the systems resources, but
    // it should be calculated somehow. Unfortunately, the actual
    // â€œhowâ€ is a closed book to me at the moment, so I just choose
    // floor(buffer_size/1.3) for the sake of choosing anything.

    /**
     * Creates a new hash node in the query tree,
     * having the two specified subqueries and the
     * join-fields.
     * @param p1 the left-hand subquery
     * @param p2 the right-hand subquery
     * @param fields the join-fields
     */
    public HashPlan(Plan p1, Plan p2, String[] fields) {
        if (tupleSize < 2) {
            // We can not work with a tuple smaller than 2 (2 means
            // that the system only has a buffer-size of 3)
            throw new RuntimeException();
        }
        this.p1 = p1;
        this.p2 = p2;
        this.fields = fields;
        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
    }

    /**
     * Creates a hash-join scan for this query.
     * @see simpledb.plan.Plan#open()
     */
    public Scan open() {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        return new HashScan(s1, s2, fields[0], fields[1], tupleSize);
    }

    /**
     * Estimates the number of block accesses in the hash-join.
     * The estimated formula is (might be wrong, though):
     * <pre> B(product(log(p1),p2)) = B(p1) + (R(p1)/tupleSize)*B(p2) </pre>
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return p1.blocksAccessed() + ((int)Math.round(p1.recordsOutput() / tupleSize) * p2.blocksAccessed());
    }

    /**
     * Estimates the number of output records in the hash-join.
     * This is impossible, since we do not know, how many equalities
     * we find over the compared fields. The best approach is an
     * estimate, which would be:
     * <pre> floor(min(R(p1), R(p2))/2) </pre>
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return (int)Math.floor(Math.min(p1.recordsOutput(), p2.recordsOutput())/2);
    }

    /**
     * Estimates the distinct number of field values in the hash-join.
     * Since my hash-join-attempt kills all multiple occurrences of
     * distinct values on the side of the smaller relation, we have no
     * other choice but to return the amount of records...
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        return recordsOutput();
    }

    /**
     * Returns the schema of the hash-join, which is the union of the
     * schemas of the underlying queries.
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return schema;
    }
    
    public String getQueryPlan(String tblname, String currQueryPlan) {
 	   return String.format("(%s hash join (index scan on %s))", currQueryPlan, tblname); 
    }
}
