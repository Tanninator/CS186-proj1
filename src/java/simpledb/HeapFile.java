package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	ArrayList<HeapPage> heapPages;
	File file;
	TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	RandomAccessFile raFile = null;
    	HeapPage returnPage = null;
    	
    	int shift = BufferPool.PAGE_SIZE * pid.pageNumber();
    
    	try {
    		raFile = new RandomAccessFile(file, "r");
    	} catch (FileNotFoundException e) {
    		//should not happen
    		e.printStackTrace();
    		System.exit(1);
    	}
    	byte[] bytes = new byte[BufferPool.PAGE_SIZE];
    	try {
    		raFile.seek(shift);
    		raFile.read(bytes, 0, BufferPool.PAGE_SIZE);
    		returnPage = new HeapPage((HeapPageId) pid, bytes);
    	} catch (IOException e) {
    		return null;
    	}
        try {
            raFile.close();
        } catch (IOException e) {
        }
    	return returnPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	double page = new Double(file.length());
    	double pageCut = Math.ceil(page/BufferPool.PAGE_SIZE);
    	return (int) pageCut;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	return new HFileIterator(getId(), numPages(), tid);
    }

}

