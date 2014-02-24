package simpledb;

import java.util.ArrayList;
import java.util.Hashtable;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op agOperator;
    private Hashtable<Integer, Integer> fieldHash;
    private Hashtable<Integer, Integer> countHash;
    private ArrayList<Tuple>tupleList;
    
    private int noGroupNum=0;
    private int numInput=0;
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	gbField = gbfield;
    	gbFieldType = gbfieldtype;
    	aField = afield;
    	agOperator = what;
    	fieldHash = new Hashtable<Integer, Integer>();
    	countHash = new Hashtable<Integer, Integer>();
    	tupleList = new ArrayList<Tuple>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	tupleList = new ArrayList<Tuple>();
    	if (gbField > -1) {
			int key = ((IntField) tup.getField(gbField)).getValue();
			int value = 0;
			if (tup.getField(aField).getType() == Type.INT_TYPE) {
				value = ((IntField) tup.getField(aField)).getValue();
			}
			if (fieldHash.containsKey(key)) {
				
				if (agOperator == Op.AVG) {
					fieldHash.put(key, fieldHash.get(key) + value);
					countHash.put(key, countHash.get(key)+1);
				} else if (agOperator == Op.MAX) {
					if (fieldHash.get(key) < value) {
						fieldHash.put(key, value);
					}
				} else if (agOperator == Op.MIN) {
					if (fieldHash.get(key) > value) {
						fieldHash.put(key, value);
					}
				} else if (agOperator == Op.SUM) {
					fieldHash.put(key, fieldHash.get(key) + value);
				} else if (agOperator == Op.COUNT) {
					countHash.put(key, countHash.get(key)+1);
				}
			} else {
				fieldHash.put(key, value);
				countHash.put(key, 1);
			}
	    	Object[] keyArray = (Object[]) fieldHash.keySet().toArray();
	    	for(int i=0; i<keyArray.length; i++) {
	    		Integer keyItem = (Integer) keyArray[i];
	    		
	    		Type[] typeArray = new Type[2];
	    		typeArray[gbField] = gbFieldType;
	    		typeArray[aField] = Type.INT_TYPE;
	    		TupleDesc td = new TupleDesc(typeArray);
	    		
	    		IntField k = new IntField(keyItem);
	    		
	    		Tuple tuple = new Tuple(td);
	    		tuple.setField(gbField, k);
	    		IntField intField = null;
	    		if (agOperator == Op.AVG) {
	    			int val = fieldHash.get(k.getValue());
	    			int divisor = countHash.get(k.getValue());
	    			val = val/divisor;
	
	        		intField = new IntField(val);
	    		} else if (agOperator == Op.COUNT) {
	    			intField = new IntField(countHash.get(k.getValue()));
	    		} else {
	    			intField = new IntField(fieldHash.get(k.getValue()));
	    		}
	    		tuple.setField(aField, intField);
	    		tupleList.add(tuple);
	    	}
    	} else { //if no_group
    		for(int i=0; i<tup.getTupleDesc().numFields(); i++) {
	    		int field = ((IntField)tup.getField(i)).getValue();
	    		if (agOperator == Op.AVG) {
	    			noGroupNum += field;
	    			numInput++;
	    		}
	    		if (agOperator == Op.MIN) {
	    			if (field < noGroupNum) {
	    				noGroupNum = field;
	    			}
	    		}
	    		if (agOperator == Op.MAX) {
	    			if (field > noGroupNum) {
	    				noGroupNum = field;
	    			}
	    		}
	    		if (agOperator == Op.COUNT) {
	    			numInput++;
	    		}
	    		if (agOperator == Op.SUM) {
	    			noGroupNum += field;
	    		}
	    		Type[] intType = {Type.INT_TYPE};
	    		TupleDesc td = new TupleDesc(intType);
	    		Tuple tuple = new Tuple(td);
	    		if (agOperator == Op.AVG) {
	    			int val = noGroupNum/numInput;
	    			IntField f = new IntField(val);
	    			tuple.setField(0, f);
	    		} else if (agOperator == Op.COUNT) {
	    			IntField f = new IntField(numInput);
	    			tuple.setField(0, f);
	    		} else {
	    			IntField f = new IntField(noGroupNum);
	    			tuple.setField(0, f);
	    		}
	    		tupleList = new ArrayList<Tuple>();
	    		tupleList.add(tuple);
    		}
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	if (gbField > -1) {
    		Type[] tpArray = {Type.INT_TYPE, Type.INT_TYPE};
    		TupleDesc td = new TupleDesc(tpArray);
    		return new TupleIterator(td, tupleList); //hold that thought
    	} else {
    		Type[] t = {Type.INT_TYPE};
    		TupleDesc td = new TupleDesc(t);
    		return new TupleIterator(td, tupleList);
    	}
    }

}
