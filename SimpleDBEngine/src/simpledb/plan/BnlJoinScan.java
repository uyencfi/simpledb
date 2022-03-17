package simpledb.plan;

import java.sql.SQLException;

import simpledb.multibuffer.ChunkScan;
import simpledb.query.*;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

/**
 * The Scan class for a Block Nested-loop Join.
 */
public class BnlJoinScan implements Scan {
    private static int COUNT = 0;

    private Transaction tx;
    private Scan lshScan, rhsScan;
    private Scan selectFromChunkScan;
    private String filename;
    private Layout layout;
    private Predicate joinPred;
    private int chunksize, nextblknum, filesize;
    private boolean isEmpty;   // flag for when either table has no records, in which case there's no point in creating ChunkScans

    /**
     * Creates a Scan for Block Nested-loop Join.
     * @param tx        the current transaction
     * @param tblname   the name of the LHS table
     * @param layout    the metadata for the LHS table
     * @param rhsScan   the RHS scan
     * @param joinPred  the predicate that joins LHS and RHS table
     */
    public BnlJoinScan(Transaction tx, String tblname, Layout layout, Scan rhsScan, Predicate joinPred, boolean lhsHasTuples) {
        COUNT = 1;
        this.tx = tx;
        this.lshScan = null;
        this.rhsScan = rhsScan;
        this.filename = tblname + ".tbl";
        this.layout = layout;
        this.joinPred = joinPred;
        this.isEmpty = !lhsHasTuples;
        filesize = tx.size(filename);
        chunksize = tx.availableBuffs() - 2;
        // System.out.println("chunksize: " + chunksize);
        if (chunksize <= 0) {
            throw new RuntimeException("Not enough buffer");
        }
        beforeFirst();
    }

    /**
     * Positions the scan before the first record, if the RHS scan is not empty.
     * That is, the LHS scan is positioned before the first record of the first chunk,
     * and the RHS scan is positioned at its first record.
     * If the RHS scan is empty, then do nothing.
     * @see Scan#beforeFirst()
     */
    @Override
    public void beforeFirst() {
        nextblknum = 0;
        rhsScan.beforeFirst();
        isEmpty = isEmpty || !rhsScan.next();   // constructor has checked if LHS is empty. Here we add another check if RHS is empty.
        if (!isEmpty) {
            useNextChunk();
        }
    }

    /**
     * Moves to the next record in the scan.
     * If there are no more records in the current chunk,
     * then move to the next LHS chunk. Both cases return true.
     * Only when there are no more LHS chunks, then return false.
     * @see Scan#next()
     */
    @Override
    public boolean next() {
        if (isEmpty) return false;

        while (!selectFromChunkScan.next()) {
            if (!useNextChunk()) {
                // System.out.println("BNL output " + COUNT + " records");
                // System.out.println("end of LHS");
                return false;
            }
        }
        COUNT++;
        return true;
    }

    // Advance to the next chunk on the LHS table
    private boolean useNextChunk() {
        assert !isEmpty;
        if (nextblknum >= filesize) {   // We've reached the end of LHS
            return false;
        }
        // Close the current chunk
        if (lshScan != null) {
            lshScan.close();
        }
        // Compute last page of next chunk
        int end = nextblknum + chunksize - 1;
        if (end >= filesize) {
            end = filesize - 1;
        }
        // Bring in the next LSH chunk. Reposition RHS table to the first record
        lshScan = new ChunkScan(tx, filename, layout, nextblknum, end);
        rhsScan.beforeFirst();
        // select from the product scan of LHS chunk and RHS table
        selectFromChunkScan = new SelectScan(new ProductScan(lshScan, rhsScan), joinPred);
        // Update next LHS chunk's first page id
        nextblknum = end + 1;
        return true;
    }

    @Override
    public int getInt(String fldname) {
        return selectFromChunkScan.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        return selectFromChunkScan.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        return selectFromChunkScan.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return selectFromChunkScan.hasField(fldname);
    }

    /**
     * Closes the current chunk scan.
     * If the Bnl scan is empty in the first place, there is nothing to close.
     * @see Scan#close()
     */
    @Override
    public void close() {
        if (isEmpty) return;
        selectFromChunkScan.close();
    }
}
