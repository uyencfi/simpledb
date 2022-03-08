package simpledb.plan;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The scan class corresponding to the <i>hash-join</i> relational
 * algebra operator.
 */
public class HashScan implements Scan {
    private Scan s1, s2;
    private HashMap<Integer, ArrayList<Constant>> tuple = new HashMap<>();
    private String fldname1, fldname2;
    private int moduloSize;

    /**
     * Creates a hash-join scan having the two underlying scans.
     * @param s1 the LHS scan
     * @param s2 the RHS scan
     * @param fldname1 the join field in LHS
     * @param fldname2 the join field in RHS
     * @param moduloSize the number of partitions (B-1)
     */
    public HashScan(Scan s1, Scan s2, String fldname1, String fldname2, int moduloSize) {
        this.s1 = s1;
        this.s2 = s2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.moduloSize = moduloSize;
        for (int i=0; i < moduloSize; i++) {
            tuple.put(i, new ArrayList<>());
        }
        s1.beforeFirst();
        pushS1();
        s2.beforeFirst();
    }

    /**
     * Private method to get the hash-tuple's fill-state.
     * @return the fill-state of the hash-tuple
     */
    private int tupleFillState() {
        int returnvalue = 0;
        for (int i=0; i<moduloSize; i++) {
            returnvalue += tuple.get(i).size();
        }
        return returnvalue;
    }

    /**
     * Private method to fill the hash-tuple with the values
     * of the smaller scan step-by-step.
     * @return false if there are no further entries in s1
     */
    private boolean pushS1() {
        boolean returnvalue = false;
        boolean parse = true;
        Constant value;
        int index = 0;
        for (int i=0; i<moduloSize; i++) {
            tuple.get(i).clear();
        }
        while (parse && (tupleFillState() < moduloSize)) {
            parse = s1.next();
            if (parse) {
                value = s1.getVal(fldname1);
                index = value.hashCode() % moduloSize;
                tuple.get(index).add(value);
                returnvalue = true;
            }
        }
        return returnvalue;
    }

    /**
     * Positions the scan before its first record.
     * The LHS scan (smaller scan) is positioned at its first
     * record, and its first (at most moduloSize) records are
     * pushed into the hash-tuple.
     * The RHS scan is positioned before its first record.
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        s1.beforeFirst();
        pushS1();
        s2.beforeFirst();
    }

    /**
     * Moves the scan to the next record.
     * The method moves to the next RHS record equal to one of the
     * hash-tuples entries. If there are none, the tuple is newly
     * filled by pushing the LHS.
     * If there are no more LHS and RHS records, the method returns
     * false.
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {
        boolean parses1 = true;
        boolean parses2 = true;
        boolean found = false;
        Constant value;
        int index;
        while (!found && parses1) {
            parses2 = s2.next();
            if (parses2) {
                value = s2.getVal(fldname2);
                index = value.hashCode() % moduloSize;
                if (tuple.get(index).contains(value)) {
                    found = true;
                }
            }
            else {
                parses1 = pushS1();
                if (parses1) {
                    s2.beforeFirst();
                }
            }
        }
        return found;
    }

    /**
     * Closes both underlying scans.
     * @see simpledb.query.Scan#close()
     */
    public void close() {
        s1.close();
        s2.close();
        tuple.clear();
    }

    /**
     * Returns the value of the specified field.
     * The value is obtained from whichever scan (hash-tuple or RHS)
     * contains the field.
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public Constant getVal(String fldname) {
        if (s1.hasField(fldname)) {
            int index = tuple.get(s2.getVal(fldname2).hashCode() % moduloSize).indexOf(s2.getVal(fldname2));
            if (index != -1) {
                return tuple.get(s2.getVal(fldname2).hashCode() % moduloSize).get(index);
            }
            else {
                // For some reason s2 has been position falsely...
                throw new RuntimeException();
            }
        }
        else
            return s2.getVal(fldname);
    }

    /**
     * Returns the integer value of the specified field.
     * The value is obtained from whichever scan (hash-tuple or RHS)
     * contains the field.
     * @see simpledb.query.Scan#getInt(java.lang.String)
     */
    public int getInt(String fldname) {
        if (s1.hasField(fldname)) {
            int index = tuple.get(s2.getVal(fldname2).hashCode() % moduloSize).indexOf(s2.getVal(fldname2));
            if (index != -1) {
                return tuple.get(s2.getVal(fldname2).hashCode() % moduloSize).get(index).asInt();
            }
            else {
                // For some reason s2 has been position falsely...
                throw new RuntimeException();
            }
        }
        else
            return s2.getInt(fldname);
    }

    /**
     * Returns the string value of the specified field.
     * The value is obtained from whichever scan (hash-tuple or RHS)
     * contains the field.
     * @see simpledb.query.Scan#getString(java.lang.String)
     */
    public String getString(String fldname) {
        if (s1.hasField(fldname)) {
            int index = tuple.get(s2.getVal(fldname2).hashCode() % moduloSize).indexOf(s2.getVal(fldname2));
            if (index != -1) {
                return tuple.get(s2.getVal(fldname2).hashCode() % moduloSize).get(index).asString();
            }
            else {
                // For some reason s2 has been position falsely...
                throw new RuntimeException();
            }
        }
        else
            return s2.getString(fldname);
    }

    /**
     * Returns true if the specified field is in
     * either of the underlying scans.
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
    public boolean hasField(String fldname) {
        return s1.hasField(fldname) || s2.hasField(fldname);
    }
}