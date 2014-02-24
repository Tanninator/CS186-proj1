package simpledb;

import java.util.ArrayList;
import java.util.Hashtable;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op agOperator;
    private Hashtable<Field, Integer> fieldHash;
    private Hashtable<Field, Integer> countHash;
    private Type aType;
    
    private ArrayList<Tuple>tupleList;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	gbField = gbfield;
    	gbFieldType = gbfieldtype;
    	aField = afield;
    	agOperator = what;
    	fieldHash = new Hashtable<Field, Integer>();
    	countHash = new Hashtable<Field, Integer>();
    	tupleList = new ArrayList<Tuple>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	tupleList = new ArrayList<Tuple>();
		Field key = tup.getField(gbField);
		int value = 0;
		aType = tup.getField(aField).getType();
		if (aType == Type.INT_TYPE) {
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
    		Field keyItem = (Field) keyArray[i];
    		
    		Type[] typeArray = new Type[2];
    		typeArray[gbField] = gbFieldType;
    		typeArray[aField] = Type.INT_TYPE;
    		TupleDesc td = new TupleDesc(typeArray);
    		
    		Tuple tuple = new Tuple(td);
    		tuple.setField(gbField, keyItem);
    		IntField intField = null;
    		if (agOperator == Op.AVG) {
    			int val = fieldHash.get(keyItem);
    			int divisor = countHash.get(keyItem);
    			val = val/divisor;

        		intField = new IntField(val);
    		} else if (agOperator == Op.COUNT) {
    			intField = new IntField(countHash.get(keyItem));
    		} else {
    			intField = new IntField(fieldHash.get(keyItem));
    		}
    		tuple.setField(aField, intField);
    		tupleList.add(tuple);
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	Type[] tpArray = {gbFieldType, Type.INT_TYPE};
    	TupleDesc td = new TupleDesc(tpArray);
        return new TupleIterator(td, tupleList);
    }

}
