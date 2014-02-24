package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    TransactionId tid;
    DbIterator tupChild;
    TupleDesc tupD;
    
    boolean hasCalled=false;
	
    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	tid = t;
    	tupChild = child;
    	Type[] i = {Type.INT_TYPE};
    	tupD = new TupleDesc(i);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	return tupD;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	tupChild.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	tupChild.close();
    	super.close();
    	hasCalled = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	hasCalled = false;
    	tupChild.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(hasCalled) {
    		return null;
    	}
    	hasCalled = true;
    	BufferPool bp = Database.getBufferPool();
    	int counter = 0;
    	while(tupChild.hasNext()) {
    		Tuple tup = tupChild.next();
    		bp.deleteTuple(tid, tup);
    		counter++;
    	}
    	IntField intF = new IntField(counter);
    	Type[] i = {Type.INT_TYPE};
    	TupleDesc td = new TupleDesc(i);
    	Tuple recCount = new Tuple(td);
    	recCount.setField(0, intF);
        return recCount;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	return new DbIterator[] { this.tupChild };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	this.tupChild = children[0];
    }

}
