package simpledb;
import java.io.Serializable;
import java.util.*;

/**
 * DbFileIterator is the iterator interface that all SimpleDB Dbfile should
 * implement.
 */
public class HFileIterator implements DbFileIterator {
	
	int tableId;
	int pageCount;
	TransactionId transId;
	HeapPage page;
	Iterator<Tuple> pgTups;
	int curPageNum=0;
	
	public HFileIterator(int tId, int noPgs, TransactionId trId) {
		tableId = tId;
		pageCount = noPgs;
		transId = trId;
	}
	
    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open()
        throws DbException, TransactionAbortedException {
    	HeapPageId pageId = new HeapPageId(tableId, curPageNum);
    	page = (HeapPage) Database.getBufferPool().getPage(transId, pageId, Permissions.READ_WRITE);
    	pgTups=page.iterator();
    	
    }

    /** @return true if there are more tuples available. */
    public boolean hasNext()
        throws DbException, TransactionAbortedException {
    	if (page == null || pgTups == null) {
    		return false;
    	}
    	if (pgTups.hasNext()) {
    		return true;
    	} else {
    		while (curPageNum < pageCount-1) {
    			if (!pgTups.hasNext()) {
        			HeapPageId pageId = new HeapPageId(tableId, curPageNum+1);  
        			page = (HeapPage) Database.getBufferPool().getPage(transId,pageId, Permissions.READ_ONLY);
        			pgTups = page.iterator();
        			curPageNum += 1;
        			if(pgTups.hasNext()) {
        				return true;
        			}
    			}
    		}
    	}	
    	return false;
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException {
    	if (hasNext()) {
    		return pgTups.next();
    	} else {
    		throw new NoSuchElementException("");
    	}
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
    	open();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
    	page=null;
    	pgTups=null;
    	pageCount=0;
    	curPageNum=0;
    }
}