package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

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

    /** Number of pages in this Buffer Pool. */
    private int NUM_PAGES;

    /** ArrayList of pages. */
    private ConcurrentHashMap<PageId, Page> pages = new ConcurrentHashMap<PageId, Page>();

    /** List of Page Accesses. Least recently used at beginning of list, most recently
    used pages at the end of the list. */
    private ConcurrentLinkedQueue<PageId> accessList = new ConcurrentLinkedQueue<PageId>();

    private ConcurrentHashMap<TransactionId, HashSet<PageId>> accessedPageMap = new ConcurrentHashMap<TransactionId, HashSet<PageId>>();

    private ConcurrentHashMap<TransactionId, HashSet<Semaphore>> heldLockMap = new ConcurrentHashMap<TransactionId, HashSet<Semaphore>>();
    private ConcurrentHashMap<PageId, Semaphore> pageLockMap = new ConcurrentHashMap<PageId, Semaphore>();
    private ConcurrentHashMap<PageId, TransactionId> writeLockMap = new ConcurrentHashMap<PageId, TransactionId>();
    private static final int NUM_PERMITS = 100;

    long DELAY = 100L; // timeout length in milliseconds


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        NUM_PAGES = numPages;
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
    	if (pageLockMap.get(pid) == null) {
    		pageLockMap.put(pid, new Semaphore(NUM_PERMITS));
    	}
        if (pages.get(pid) != null) {
            getLock(tid, pid, perm);
            markPageAccess(pid, tid);
            return pages.get(pid);
        }
        if (pages.size() == NUM_PAGES) {
            evictPage();
        }
        Catalog catalog = Database.getCatalog();
        Page page = null;
        DbFile dbFile = catalog.getDbFile(pid.getTableId());
        try {
        	page = dbFile.readPage(pid);
        } catch (IllegalArgumentException iae) {
        	System.out.println("illegal argument exception in getPage");
        	// should not happen?
        }

        pages.put(pid, page);
        getLock(tid, pid, perm);
        markPageAccess(pid, tid);
        return page;
    }

    /**
     * Helper method to mark a page as accessed for LRU purposes.
     * Also track all pages a transaction has accessed.
     */
    private void markPageAccess(PageId pid, TransactionId tid) {
        if (accessList.contains(pid)) {
            accessList.remove(pid);
            accessList.offer(pid);
        } else {
            accessList.offer(pid);
        }

        if (accessedPageMap.get(tid) == null) {
            HashSet<PageId> pageIdSet = new HashSet<PageId>();
            pageIdSet.add(pid);
            accessedPageMap.put(tid, pageIdSet);
        } else {
            accessedPageMap.get(tid).add(pid);
        }
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
    public void releasePage(TransactionId tid, PageId pid) {
    	if (! holdsLock(tid, pid)) {
    		System.out.println("transaction " + tid + " does not hold a lock on page " + pid);
    		return;
    	}
    	Semaphore semaphore = pageLockMap.get(pid);
    	HashSet<Semaphore> heldSemaphores = heldLockMap.get(tid);
    	TransactionId writeLockHolder = writeLockMap.get(pid);
    	if (writeLockHolder != null && writeLockHolder.equals(tid)) {
    		semaphore.release(NUM_PERMITS);
    		writeLockMap.remove(pid);
    		accessedPageMap.get(tid).remove(pid);
    		return;
    	}
    	if (heldLockMap.get(tid).contains(semaphore)) {
    		semaphore.release();
    		heldSemaphores.remove(semaphore);
    		assert(!heldLockMap.get(tid).contains(semaphore));
    	}
    	accessedPageMap.get(tid).remove(pid);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
    	return holdsReadLock(tid, p) || holdsWriteLock(tid, p);
    }

    public boolean holdsReadLock(TransactionId tid, PageId p) {
    	if (heldLockMap.get(tid) == null) {
    		heldLockMap.put(tid, new HashSet<Semaphore>());
    		return false;
    	}
    	Semaphore semaphore = pageLockMap.get(p);
    	return heldLockMap.get(tid).contains(semaphore);
    }

    public boolean holdsWriteLock(TransactionId tid, PageId p) {
    	if (heldLockMap.get(tid) == null) {
    		heldLockMap.put(tid, new HashSet<Semaphore>());
    		return false;
    	}
    	TransactionId writeLockHolder = writeLockMap.get(p);
    	if (writeLockHolder == null) {
    		return false;
    	}
    	return writeLockMap.get(p).equals(tid);
    }

    public synchronized void getLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
    	if (perm == Permissions.READ_ONLY) {
    		if (holdsLock(tid, pid)) {
    			return;
    		}
    		Semaphore semaphore = pageLockMap.get(pid);
    		try {
    			boolean successfullyAcquired = semaphore.tryAcquire(DELAY, TimeUnit.MILLISECONDS);
    			if (! successfullyAcquired) {
    				System.out.println("Timed out trying to acquire read lock.");
    				throw new TransactionAbortedException();
    			}
    			heldLockMap.get(tid).add(semaphore);
    		} catch (InterruptedException ie) {
    			System.out.println("interruptedException when trying to get a read lock for transaction " + tid + " of page " + pid);
                throw new TransactionAbortedException();
    		}
    	} else if (perm == Permissions.READ_WRITE) {
    		if (holdsWriteLock(tid, pid)) {
    			return;
    		}
    		Semaphore semaphore = pageLockMap.get(pid);
    		if (holdsReadLock(tid, pid) && NUM_PERMITS - semaphore.availablePermits() == 1) {
    			semaphore.release();
    		}
    		try {
    			if (! holdsWriteLock(tid, pid)) {

    				boolean successfullyAcquired = semaphore.tryAcquire(NUM_PERMITS, DELAY, TimeUnit.MILLISECONDS);
    				if (! successfullyAcquired) {
    					System.out.println("Timed out trying to acquire write lock  for page " + pid);
    					throw new TransactionAbortedException();
    				}
    				if (semaphore.availablePermits() != 0) {
    					System.out.println("nonzero permits");
    					System.out.println(semaphore.availablePermits());
    				}
    				assert(semaphore.availablePermits() == 0);
    				writeLockMap.put(pid, tid);
    			}
    		} catch (InterruptedException ie) {
    			System.out.println("interrupted exceptionwhen trying to get a write lock for transaction " + tid + " of page " + pid);
                throw new TransactionAbortedException();
    		}
    	} else {
    		System.err.println("Permission was not read only or read write");
    		//probably unnecessary but just in case
    	}
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
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
        // Take care of flushing or discarding pages here
        if (commit) {
            flushPages(tid);
        } else {
        	if (accessedPageMap.get(tid) == null ) {
        		System.out.println("no accessed pages");
        		return;
        	}
        	HashSet<PageId> accessedPages = new HashSet<PageId>(accessedPageMap.get(tid));
        	if (accessedPages == null) {
        		return;
        	}
            for (PageId p : accessedPages) {
                discardPage(p);
                accessedPageMap.get(tid).remove(p);
            }
        }

        // Take care of locks
        if (heldLockMap.get(tid) == null) {
        	return;
        }
        HashSet<Semaphore> heldSemaphores = new HashSet<Semaphore>(heldLockMap.get(tid));
        if (heldSemaphores == null) {
        	return;
        }
        releaseWriteLocks(tid);
        for (Semaphore s : heldSemaphores) {
            s.release();
            heldLockMap.get(tid).remove(s);
        }
    }

    private synchronized void releaseWriteLocks(TransactionId tid) {
    	HashSet<PageId> pageIdSet = new HashSet<PageId>(writeLockMap.keySet());
    	for (PageId pid : pageIdSet) {
    		if (writeLockMap.get(pid).equals(tid)) {
    			Semaphore pageLock = pageLockMap.get(pid);
    			pageLock.release(NUM_PERMITS);
    			heldLockMap.get(tid).remove(pageLock);
    			writeLockMap.remove(pid);
    		}
    	}
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
    	HeapFile heapFile = (HeapFile) Database.getCatalog().getDbFile(tableId);
    	heapFile.insertTuple(tid, t);
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
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
    	DbFile file = Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId());
    	return file.deleteTuple(tid, t);

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : pages.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        if (pages.containsKey(pid)) {
            pages.remove(pid);
            accessList.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
        file.writePage(pages.get(pid));
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        HashSet<PageId> accessed = accessedPageMap.get(tid);
        if (accessed == null) {
            return;
        }
    	HashSet<PageId> transactionPages = new HashSet<PageId>(accessed);
        for (PageId p : transactionPages) {
            if (holdsWriteLock(tid, p)) {
                flushPage(p);
                accessedPageMap.get(tid).remove(p);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageId removePage = null;
        Iterator<PageId> iter = accessList.iterator();
        while (iter.hasNext()) {
            PageId currentPageId = iter.next();
            Page currentPage = pages.get(currentPageId);
            if (null == currentPage.isDirty()) {
                discardPage(currentPageId);
                removePage = currentPageId;
                removePageAccess(currentPageId);
                break;
            }
        }

        if (removePage == null) {
            // No clean pages were found, so throw exception
            throw new DbException("Unable to evict page because all pages are dirty.");
        } else {
            accessList.remove(removePage);
        }
    }

    private synchronized void removePageAccess(PageId pid) {
    	HashSet<TransactionId> tidSet = new HashSet<TransactionId>(accessedPageMap.keySet());
    	for (TransactionId tid : tidSet) {
    		if (accessedPageMap.get(tid).contains(pid)) {
    			accessedPageMap.get(tid).remove(pid);
    		}
    	}
    }

}
