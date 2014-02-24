package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator childIter;
    private int aField;
    private int gField;
    private Aggregator.Op agOperator;
    private TupleDesc fieldTypeDesc;
    private Type fieldType;
    private Type aggFieldType;
    private Hashtable<Field,Integer> aggHash;
    private Hashtable<Field, Integer> countHash;
    
    private ArrayList<Tuple> fieldList;
    private int index;
    private Aggregator agg;
    private DbIterator endIter;
    
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
    	childIter = child;
    	aField = afield;
    	gField = gfield;
    	agOperator = aop;
    	
    	fieldTypeDesc = child.getTupleDesc();
    	if (gField > -1) {
    		fieldType = fieldTypeDesc.getFieldType(gField);
    	}
    	aggFieldType = fieldTypeDesc.getFieldType(aField);
    	
    	if (fieldType == Type.STRING_TYPE) {
    		agg = new StringAggregator(gField, fieldType, aField, aop);
    	} else {
    		agg = new IntegerAggregator(gField, fieldType, aField, aop);
    	}
    	
    	aggHash = new Hashtable<Field, Integer>();
    	countHash = new Hashtable<Field, Integer>();
    	
    	fieldList = new ArrayList<Tuple>();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	return gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return null;
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return agOperator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    	childIter.open();
    	super.open();
    	while(childIter.hasNext()) {
    		Tuple current = childIter.next();
    		agg.mergeTupleIntoGroup(current);
    	}
    	endIter = agg.iterator();
    	endIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
    	if (endIter != null && endIter.hasNext()) {
    		return endIter.next();
    	} else {
    		return null;
    	}
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    	childIter.rewind();
    	endIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	return childIter.getTupleDesc(); //pretty sure its wrong right now
    }

    public void close() {
	// some code goes here
    	childIter.close();
    	endIter.close();
    	if (fieldType == Type.INT_TYPE) {
    		agg = new IntegerAggregator(gField, fieldType, aField, agOperator);
    	} else {
    		agg = new StringAggregator(gField, fieldType, aField, agOperator);
    	}
    	
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
	return new DbIterator[] { this.childIter };
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
    	this.childIter = children[0];
    }
    
}
