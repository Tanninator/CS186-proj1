package simpledb;

import java.io.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private LinkedList<PageId> orderUsed = new LinkedList<PageId>();
    private HashMap<PageId, Page> pages;
    private HashMap<TransactionId, ArrayList<PageId>> tIdtopId = new HashMap<TransactionId, ArrayList<PageId>>();
    int maxPageCount;
    

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
    	maxPageCount = numPages;
        pages = new HashMap<PageId, Page>(numPages);
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        if(pages.containsKey(pid)) {
        	orderUsed.remove(pid);
        	orderUsed.add(pid);
        	return pages.get(pid);
        }
        while(pages.size() > maxPageCount) {
    		evictPage();
    	}
    	Catalog c = Database.getCatalog();
        Page page = null;
        DbFile dbFile = c.getDbFile(pid.getTableId());
        try {
        	page = dbFile.readPage(pid);
        } catch (IllegalArgumentException iae) {
        }
    	pages.put(pid, page);
    	ArrayList<PageId> pList = tIdtopId.get(tid);
    	if (pList != null) {
    		pList.add(pid);
    	} else {
    		ArrayList<PageId> newPageList = new ArrayList<PageId>();
    		newPageList.add(pid);
    		tIdtopId.put(tid, newPageList);
    	}
    	orderUsed.add(pid);
    	return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	HeapFile heapFile = (HeapFile) Database.getCatalog().getDbFile(tableId);
    	heapFile.insertTuple(tid, t); //dirtiness marker delegated to heapFile insert
        // not necessary for proj1
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
    	DbFile file = Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId());
    	file.deleteTuple(tid, t); //dirtiness marker delegated to heapFile insert
        // not necessary for proj1
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
    	for(PageId key: pages.keySet()) {
    		flushPage(key);
    	}
        // not necessary for proj1

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1
    	if (pages.containsKey(pid)) {
    		pages.remove(pid);
    		orderUsed.remove(pid);
    	}
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
    	DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
    	file.writePage(pages.get(pid));
        // not necessary for proj1
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    	if (tIdtopId.containsKey(tid)) {
    		ArrayList<PageId> pIds = tIdtopId.get(tid);
    		for(PageId pId: pIds) {
    			flushPage(pId);
    			orderUsed.remove(pId);
    		}
    	}
    		
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
    	PageId pid = orderUsed.poll();
    	pages.remove(pid);
    }

}
