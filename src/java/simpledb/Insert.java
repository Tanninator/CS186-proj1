package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    TransactionId tid;
    DbIterator tupChild;
    int tableId;
    TupleDesc tupD;
    
    boolean hasCalled=false;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	tid = t;
    	tupChild = child;
    	tableId = tableid;
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
    	tupChild.rewind();
    	hasCalled = false;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
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
    		try {
    			bp.insertTuple(tid, tableId, tup);
    			counter++;
    		} catch (IOException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
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
