package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate pred;
    DbIterator childOp;
    TupleDesc tDesc;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        pred = p;
        childOp = child;
        tDesc = childOp.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return pred;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	return tDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	childOp.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	childOp.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	try {
    		childOp.rewind();
    	} catch (IllegalStateException i) {
    		//derp
    	}
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	while(childOp.hasNext()) { //filter by predicate
    		Tuple tuple = childOp.next();
    		if (pred.filter(tuple)) {
    			return tuple;
    		}
    	}
    	return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	return new DbIterator[] { this.childOp };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	this.childOp = children[0];
    }

}
