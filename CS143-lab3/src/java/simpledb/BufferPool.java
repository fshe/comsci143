package simpledb;

import java.io.*;

import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    private Page[] pages;
    private int maxPages;
    private int currentPage;
    private int mru;
    public BufferPool(int numPages) {
        pages = new Page[numPages];
	maxPages = numPages;
	currentPage = 0;
	mru = 0;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
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
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
	for( int i = 0; i < currentPage; i++)
	{
		if (pages[i].getId().equals(pid))
		{
			mru = i;
			return pages[i];
		}
	}
	if (currentPage == maxPages) //mru: replace most recently used page
	{
		evictPage();
		int tableid = pid.getTableId();
		DbFile file = Database.getCatalog().getDatabaseFile(tableid);
		Page temp = file.readPage(pid);
		pages[mru] = temp;
		return temp;
	}
	mru = currentPage;
	int tableid = pid.getTableId();
	DbFile file = Database.getCatalog().getDatabaseFile(tableid);
	Page temp = file.readPage(pid);
	pages[currentPage] = temp;
	currentPage++;
	return temp;
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
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
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
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
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
	DbFile file = Database.getCatalog().getDatabaseFile(tableId);
	Page page = file.insertTuple(tid, t).get(0);
	page.markDirty(true, tid);	
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
	DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        try
        {
    	int tableId = t.getRecordId().getPageId().getTableId(); 
    	DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
    	HeapFile heapFile = (HeapFile)dbFile;
    	Page affectedPage = heapFile.deleteTuple(tid, t).get(0);
    	//iterate through affectedPages and markDirty
        affectedPage.markDirty(true,tid);
        }
        catch (DbException e){
                e.printStackTrace();
            }
        catch (TransactionAbortedException e){
                e.printStackTrace();
            }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for ( int i = 0; i < currentPage; i++)
        {
                flushPage(pages[i].getId());
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
	//get the page
	Page page = null;
        for( int i = 0; i < currentPage; i++)
        {
                if (pages[i].getId().equals(pid))
                {
                        page = pages[i];
			int tableid = ((HeapPageId) pid).getTableId();
			HeapFile f = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
			f.writePage(page);
			page.markDirty(false,null);
                }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
	for ( int i = 0; i < currentPage; i++)
	{
		if(pages[i].isDirty() == tid)
			flushPage(pages[i].getId());
	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
                PageId evictPid = pages[mru].getId();
                try{
                flushPage(evictPid);
                }
                catch(IOException exception){}

    }

}
