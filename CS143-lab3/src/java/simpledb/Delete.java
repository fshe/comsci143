package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private TransactionId m_t;
    private DbIterator m_child;
    private TupleDesc td;
    private static final long serialVersionUID = 1L;
    private boolean m_twice;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        m_child = child;
	m_t = t;
	m_twice = false;
        Type[] type = new Type[1];
        String[] string = new String[1];
        type[0] = Type.INT_TYPE;
        string[0] = "Number Deleted";
        td = new TupleDesc(type, string);
    }

    public TupleDesc getTupleDesc() {
	return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        m_child.open();
	super.open();
    }

    public void close() {
        super.close();
	m_twice = true;
	m_child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
	m_twice = false;
        m_child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        Tuple returnTuple = new Tuple(td);
        int count = 0;
        if(m_twice)
                return null;
        m_twice = true;
        while( m_child.hasNext())
        {
                Tuple tup = m_child.next();
                try{
                Database.getBufferPool().deleteTuple(m_t, tup);
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
	return new DbIterator[] { m_child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        m_child = children[0];
    }

}
