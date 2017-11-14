package simpledb;
import java.io.*;
import java.util.*;
/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId m_t;
    private DbIterator m_child;
    private int m_tid;
    private TupleDesc td;
    private boolean m_twice;
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
	if (!Database.getCatalog().getTupleDesc(tableid).equals(child.getTupleDesc()))
		throw new DbException("Tuple descriptors do not match");
	m_t = t;
	m_child = child;
	m_tid = tableid;	
	m_twice = false;

        Type[] type = new Type[1];
        String[] string = new String[1];
        type[0] = Type.INT_TYPE;
        string[0] = "Number Inserted";
        td = new TupleDesc(type, string);
    }

    public TupleDesc getTupleDesc() {
	return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
	m_child.open();
	super.open();
    }

    public void close() {
        // some code goes here
	super.close();
	m_twice = true;
	m_child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
	m_twice = false;
	m_child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
	Tuple returnTuple = new Tuple(td);
	int count = 0;
	if(m_twice)
		return null;
	m_twice = true;
	while( m_child.hasNext())
	{
		Tuple tup = m_child.next();
		try{
		Database.getBufferPool().insertTuple(m_t, m_tid, tup);
		}
		catch (IOException exception){}
		count++;
	}
	Field field = new IntField(count);
	returnTuple.setField(0, field);
	return returnTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { m_child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
	m_child = children[0];
    }
}
