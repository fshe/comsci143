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

    private File file;
    private TupleDesc td;
    int id;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
	file = f;
	this.td = td;
	id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
	return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
	return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
	int pageSize = Database.getBufferPool().getPageSize();
	int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;
	try {
	RandomAccessFile random = new RandomAccessFile(file, "r");
	byte[] buffer = new byte[BufferPool.PAGE_SIZE];
	random.seek(offset);
	random.read(buffer, 0, BufferPool.PAGE_SIZE);
	HeapPageId hid = (HeapPageId) pid;
	random.close();

        return new HeapPage(hid, buffer);
	}
	catch (IOException exception) {return null;}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
	try {
		PageId pid = page.getId();
		RandomAccessFile random = new RandomAccessFile(file,"rw");
		int offset = pid.pageNumber()*BufferPool.PAGE_SIZE;
		byte[] buffer = new byte[BufferPool.PAGE_SIZE];
		buffer = page.getPageData();
		random.seek(offset);
		random.write(buffer, 0, BufferPool.PAGE_SIZE);
		random.close();          
        } 
	catch (IOException exception){}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
	int pageSize = Database.getBufferPool().getPageSize();
	if( (int) file.length() % pageSize == 0)
		return (int) file.length()/pageSize;
        return ((int) file.length()/pageSize)+1;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
	ArrayList<Page> modifiedPages = new ArrayList<Page>();
	//get bufferpool
	BufferPool B = Database.getBufferPool();
	for (int i = 0; i < numPages(); i++)
	{
		HeapPageId hpid =  new HeapPageId(getId(), i);
		HeapPage hp = (HeapPage) B.getPage(tid, hpid, Permissions.READ_WRITE);
		if (hp.getNumEmptySlots() > 0)
		{
			hp.insertTuple(t);
			modifiedPages.add(hp);
			return modifiedPages;
		}
	}
	//otherwise there are no empty pages, add one
	HeapPageId pid = new HeapPageId(getId(), numPages());
	HeapPage page = (HeapPage) B.getPage(tid, pid, Permissions.READ_WRITE);
	page.insertTuple(t);
	writePage(page);
	modifiedPages.add(page);
	return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
	RecordId rid = t.getRecordId();
	PageId pid = rid.getPageId();
	Page p = Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE); //get page
	HeapPage hp = (HeapPage) p;
	hp.deleteTuple(t); //delete the tuple from page
	ArrayList<Page> modifiedPages = new ArrayList<Page>();
	modifiedPages.add(Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY));
	return modifiedPages;
    }

    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator( tid, this );
    }

}

