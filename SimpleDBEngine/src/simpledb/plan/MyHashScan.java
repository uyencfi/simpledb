package simpledb.plan;

import java.util.ArrayList;
import java.util.List;

import simpledb.materialize.TempTable;
import simpledb.multibuffer.MultibufferProductScan;
import simpledb.query.*;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class MyHashScan implements Scan {
    /* Primes for hashing (used in partition phase). */
    private static final int[] PRIMES = {17, 31, 53, 67, 89, 131, 173, 211, 269, 331, 379, 431, 467, 499};
    private int PRIME1;
    private int PRIME2;

    /* Attributes of the tables being joined. */
    private Transaction tx;
    private Scan L, R;
    private Schema lSchema, rSchema;
    private String lField, rField;

    /* Necessary info for partitioning */
    private int numBuff;    // Number of buffers available
    private TempTable[] lPartitions, rPartitions;    // Arrays holding the partitions of the left and right scans
    private List<Integer> failedPartitionNums;   // Some partition pairs are too big, and need to be recursively hash joined.
                                                 // This list keeps track of those partitions.

    int currPartition;   // Current partition cursor. We lazy join, so we need to know which partition is currently being joined/
    Scan currentScan;     // The scan that will yield resultant records. This scan is built from joining the partitions.

    /**
     * Creates a hash join scan from the two underlying scans.
     * @param tx the calling transaction
     * @param left the LHS scan
     * @param right the RHS scan
     * @param lSchema the schema of the LHS scan
     * @param rSchema the schema of the RHS scan
     * @param lField the join field in LHS
     * @param rField the join field in RHS
     */
    public MyHashScan(Transaction tx, Scan left, Scan right, Schema lSchema, Schema rSchema, String lField, String rField) {
        this.tx = tx;
        this.L = left;
        this.R = right;
        this.lSchema = lSchema;
        this.rSchema = rSchema;
        this.lField = lField;
        this.rField = rField;

        numBuff = tx.availableBuffs();
        failedPartitionNums = new ArrayList<>();

        currPartition = 0;
        currentScan = null;

        choosePrimesForHash();
        runPartitionPhase();
        // runJoinPhase();
        JoinMore();
    }

    // --------- Methods for Partition Phase -------------------------------

    /**
     * Partitions the left and right scans.
     */
    private void runPartitionPhase() {
        lPartitions = partition(L, lField);
        rPartitions = partition(R, rField);
    }

    /**
     * Hash function used for partitioning.
     * @param val        the value of the field to be hashed
     * @param numBuckets the number of buckets in the partition
     * @return the bucket this value should go into.
     */
    private int hashFunc1(Constant val, int numBuckets) {
        int h = val.hashCode();
        return ((h * PRIME1) % PRIME2) % numBuckets;
    }

    /**
     * Generate partitions of a scan.
     *
     * @param s                     the scan to be partitioned
     * @param fieldName             the field name of the attribute to be hashed
     * @return the partitions as an array of temporary tables
     */
    private TempTable[] partition(Scan s, String fieldName) {
        s.beforeFirst();
        boolean isLeft = (s == L);
        Schema sch = (isLeft) ? lSchema : rSchema;

        // Open scans for k temporary tables (they act as buffers when we partition)
        TempTable[] partitions = new TempTable[numBuff - 1];
        UpdateScan[] tempScans = new UpdateScan[numBuff -1];
        for (int i = 0; i < numBuff - 1; i++) {
            partitions[i] = new TempTable(tx, sch);
            tempScans[i] = partitions[i].open();
        }

        // Hash records one by one, and put them in their partition
        while (s.next()) {
            int bucket = hashFunc1(s.getVal(fieldName), numBuff - 1);
            UpdateScan partition = tempScans[bucket];
            partition.insert();
            for (String f : sch.fields()) {
                partition.setVal(f, s.getVal(f));
            }
        }

        // Clean up: close the temporary table scans, close the source table scan
        for (int i = 0; i < numBuff - 1; i++) {
            tempScans[i].close();
        }
        s.close();
        return partitions;
    }


    // --------- Methods for Join Phase -------------------------------

    /**
     * Runs the join phase. Iterates over the array of partitions and joins those
     * that fit in memory first (skipping over those that are too large). Only when all
     * in-memory partitions have been joined, will it run recursive hash join on the
     * too-big partitions.
     */
    private void JoinMore() {
        for (int i = currPartition; i < lPartitions.length; i++) {
            TempTable lTable = lPartitions[i];
            TempTable rTable = rPartitions[i];

            TempTable fit = null, other = null;
            if (tx.size(lTable.tableName()) < numBuff - 2) {
                fit = lTable;
                other = rTable;
            } else if (tx.size(rTable.tableName()) < numBuff - 2) {
                fit = rTable;
                other = lTable;
            }

            if (fit != null) {      // Found a small enough partition
                currPartition = i + 1;
                if (currentScan != null) currentScan.close();
                // TODO implement hashmap
                currentScan = new SelectScan(
                        new MultibufferProductScan(tx, other.open(), fit.tableName(), fit.getLayout()),
                        new Predicate(new Term(new Expression(lField), new Expression(rField), "=")));
                currentScan.beforeFirst();
                return;
            } else {        // This pair of partition is too big, skip them. We'll recursively HJ them later.
                // System.out.println("Neither the left nor the right partition " + i + " fits in B-2 buffers");
                failedPartitionNums.add(i);
                currPartition = i + 1;
            }
        }

        // System.out.println("Iterated through all partitions");
        // TODO is currentScan ever null? Yes, when all partitions are too big. All partitions are in failed
        // currentScan = null;
        if (failedPartitionNums.isEmpty()) {      // no failed partitions. DONE !
            // System.out.println("No more failed partitions");
            // TODO should close ?
            currentScan.close();
            currentScan = null;
        } else {       // else, there are some failed partitions. Cannot stop yet. Must also join them!
            recursiveHashJoin();
        }
    }

    /**
     * Recursively applies Hash Join to the partitions that were too big to fit in memory.
     * Assumption: at least 1 failed partition remaining.
     */
    private void recursiveHashJoin() {
        assert failedPartitionNums.size() >= 1;
        int failedNum = failedPartitionNums.get(0);
        if (currentScan != null) {
            currentScan.close();
        }
        Scan leftPartition = lPartitions[failedNum].open();
        Scan rightPartition = rPartitions[failedNum].open();
        currentScan = new MyHashScan(tx, leftPartition, rightPartition, lSchema, rSchema, lField, rField);
        currentScan.beforeFirst();
        failedPartitionNums.remove(0);
    }


    // --------- Methods implementing Scan interface ----------------------

    /**
     * Moves the result scan to the next record.
     * @return true if there is a next record, false if there are no more records.
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {
        // System.out.println("curr partition: " + currPartition + ", buff=" + numBuff);
        if (currentScan == null) {     // no more partitions
            return false;
        }
        if (currentScan.next())
            return true;
        else {
            // currentScan.close();
            JoinMore();
            return next();
        }
    }

    /**
     * Positions the scan before the first record.
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        assert currentScan != null;
        currentScan.beforeFirst();
    }

    /**
     * Closes all underlying scans (actually they are already closed
     * by other methods so no need to do anything here).
     * @see simpledb.query.Scan#close()
     */
    public void close() {
//         if (currentScan != null) currentScan.close();
//         L.close();
//         R.close();
    }

    /**
     * Returns the value of the specified field as a Constant.
     * The value is obtained from whichever scan
     * contains the field.
     * @see simpledb.query.Scan#getVal(String)
     */
    public Constant getVal(String fldname) {
        assert currentScan != null;
        return currentScan.getVal(fldname);
    }

    /**
     * Returns the integer value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * @see simpledb.query.Scan#getInt(String)
     */
    public int getInt(String fldname) {
        assert currentScan != null;
        return currentScan.getInt(fldname);
    }

    /**
     * Returns the string value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * @see simpledb.query.Scan#getString(String)
     */
    public String getString(String fldname) {
        assert currentScan != null;
        return currentScan.getString(fldname);
    }

    /**
     * Returns true if the specified field is in
     * either of the underlying scans.
     * @see simpledb.query.Scan#hasField(String)
     */
    public boolean hasField(String fldname) {
        assert currentScan != null;
        return currentScan.hasField(fldname);
    }

    // --------- Methods to choose the primes ----------------------

    private static int randPrime() {
        int index = (int) (Math.random() * (PRIMES.length - 1));
        return PRIMES[index];
    }

    private void choosePrimesForHash() {
        PRIME1 = randPrime();
        int tempPrime = randPrime();
        while (tempPrime == PRIME1) {
            tempPrime = randPrime();
        }
        PRIME2 = tempPrime;
    }
}