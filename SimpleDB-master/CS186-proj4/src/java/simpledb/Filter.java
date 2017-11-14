package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
    
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    private Predicate m_p;
    private DbIterator m_child;
    private TupleDesc td;
    private ArrayList<Tuple> childTups = new ArrayList<Tuple>();
    private Iterator<Tuple> it;

    public Filter(Predicate p, DbIterator child) {
        m_p = p;
	m_child = child;
	td = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        return m_p;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
	m_child.open();
//code to make a iterator over the proper collection
	while(m_child.hasNext())
	{
		Tuple temp = (Tuple) m_child.next();
		if (getPredicate().filter(temp))
			childTups.add(temp);
	}
	it = childTups.iterator();
	super.open();
    }

    public void close() {
        super.close();
	it = null;
	//child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        //child.rewind();
	it = childTups.iterator();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
	if( it!= null && it.hasNext())
		return it.next();
	return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.m_child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (this.m_child!=children[0])
        {
            this.m_child = children[0];
        }
    }

}
