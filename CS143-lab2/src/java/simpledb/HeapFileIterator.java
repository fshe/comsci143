package simpledb;

import java.io.*;
import java.util.*;

public class HeapFileIterator implements DbFileIterator {
    private HeapFile file;
    private TransactionId tid;
    private int currentPage;
    private int numPages;
    private Iterator<Tuple> iter;
    private HeapPage page;
    private int tableId;
        
    public HeapFileIterator(TransactionId tid, HeapFile file){
	this.file = file;
	this.tid = tid;
	currentPage = 0;
	numPages = file.numPages();
	tableId = file.getId();
    }
    public void open() 
        throws TransactionAbortedException, DbException {
	if (iter != null)
		throw new TransactionAbortedException();
	HeapPageId pid = new HeapPageId(tableId, currentPage++);
	page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
	iter = page.iterator();
    }

    public boolean hasNext() 
	throws DbException, TransactionAbortedException
    {
        if ( iter == null )
                return false;
        if ( iter.hasNext() )
                return true;
        while(currentPage <= (numPages - 1))
        {
		HeapPageId pid = new HeapPageId(tableId, currentPage++);
		page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                iter = page.iterator();
                if (iter.hasNext())
                        return true;
        }
        return false;
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        if (iter == null)
                throw new NoSuchElementException("Iterator has not been opened");
        if (!iter.hasNext())
                throw new NoSuchElementException("No more tuples exist");
        return iter.next();
    }


    public void close() {
        iter = null;
        currentPage = 0;
    }

    public void rewind() 
	throws TransactionAbortedException, DbException
    {
        close();
	open();
    }
}
