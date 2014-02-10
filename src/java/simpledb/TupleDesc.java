package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	ArrayList<TDItem> itemList;
	
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return itemList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	for(int i=0; i<typeAr.length; i++) {
    		TDItem item = new TDItem(typeAr[i], fieldAr[i]);
    		itemList.add(item);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	for(int i=0; i<typeAr.length; i++) {
    		TDItem item = new TDItem(typeAr[i], null);
    		itemList.add(item);
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return itemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	if (i<itemList.size()) {
    		return itemList.get(i).fieldName;
    	} else {
    		throw new NoSuchElementException();
    	}
    }
    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if (i<itemList.size()) {
    		return itemList.get(i).fieldType;
    	} else {
    		throw new NoSuchElementException();
    	}
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for(int i=0; i<itemList.size(); i++) {
        	if (itemList.get(i).fieldName.equals(name)) {
        		return i;
        	}
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return itemList.size();
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] typeList = new Type[td1.getSize()+td2.getSize()];
        String[] nameList = new String[td1.getSize()+td2.getSize()];
        for(int i=0; i<td1.getSize(); i++) {
        	typeList[i] = td1.getFieldType(i);
        	nameList[i] = td1.getFieldName(i);
        }
        for(int i=0; i<td2.getSize(); i++) {
        	typeList[i+td1.getSize()] = td2.getFieldType(i);
        	nameList[i+td1.getSize()] = td2.getFieldName(i);
        }
        return new TupleDesc(typeList, nameList);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)) {
        	return false;
        }
        if (getSize() != ((TupleDesc)o).getSize()) {
        	return false;
        }
        Iterator myIt = iterator();
        Iterator oIt = ((TupleDesc)o).iterator();
        while(myIt.hasNext()) {
        	if(!myIt.next().toString().equals(oIt.next().toString())) {
        		return false;
        	}
        }
        return true;
    }

    public int hashCode() {
    	return toString().hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	String endString = "";
    	Iterator myIt = iterator();
    	while(myIt.hasNext()) {
        	endString += myIt.next().toString() + " , ";
        }
        return endString;
    }
}
