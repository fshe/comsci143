package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

     private static final long serialVersionUID = 1L;

     private DbIterator m_child;
     private int m_afield;
     private int m_gfield;
     private Aggregator.Op m_op;
     private TupleDesc m_td;
     private Aggregator m_agg;
     private DbIterator m_aggregates;

     /**
      * Constructor.
      * 
      * Implementation hint: depending on the type of afield, you will want to
      * construct an {@link IntAggregator} or {@link StringAggregator} to help
      * you with your implementation of readNext().
      * 
      * 
      * @param child
      *            The DbIterator that is feeding us tuples.
      * @param afield
      *            The column over which we are computing an aggregate.
      * @param gfield
      *            The column over which we are grouping the result, or -1 if
      *            there is no grouping
      * @param aop
      *            The aggregation operator to use
      */
     public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	  // some code goes here DONE
	  m_child = child;
	  m_afield = afield;
	  m_gfield = gfield;
	  m_op = aop;
	  m_td = child.getTupleDesc();


	  Type t1, t2;

	  // check for no grouping
	  if ( gfield == Aggregator.NO_GROUPING )
	       t1 = null;
	  else 
	       t1 = m_td.getFieldType( gfield );

	  t2 = m_td.getFieldType( afield );
	  
	  
	  // check the type to check which aggregator to use
	  if ( t2 == Type.STRING_TYPE )
	       m_agg = new StringAggregator( gfield, t1, afield, aop );
	  else
	       m_agg = new IntegerAggregator( gfield, t1, afield, aop );

     }

     /**
      * @return If this aggregate is accompanied by a groupby, return the groupby
      *         field index in the <b>INPUT</b> tuples. If not, return
      *         {@link simpledb.Aggregator#NO_GROUPING}
      * */
     public int groupField() {
	  // some code goes here DONE
	  return m_gfield;
     }

     /**
      * @return If this aggregate is accompanied by a group by, return the name
      *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
      *         null;
      * */
     public String groupFieldName() {
	  // some code goes here DONE
	  return m_td.getFieldName( m_gfield );
     }

     /**
      * @return the aggregate field
      * */
     public int aggregateField() {
	  // some code goes here DONE
	  return m_afield;
     }

     /**
      * @return return the name of the aggregate field in the <b>OUTPUT</b>
      *         tuples
      * */
     public String aggregateFieldName() {
	  // some code goes here DONE
	  return m_td.getFieldName( m_afield );
     }

     /**
      * @return return the aggregate operator
      * */
     public Aggregator.Op aggregateOp() {
	  // some code goes here DONE
	  return m_op;
     }

     public static String nameOfAggregatorOp(Aggregator.Op aop) {
	  return aop.toString();
     }

     public void open() throws NoSuchElementException, DbException,
	  TransactionAbortedException {
	  // some code goes here DONE
	  m_child.open();
	  Tuple t;

	  // merge all tuples into aggregates
	  while ( m_child.hasNext() )
	  {
	       t = m_child.next();
	       m_agg.mergeTupleIntoGroup( t );
	  }
	  
	  // get the iterator to the aggregate values and open it
	  m_aggregates = m_agg.iterator();

	  m_aggregates.open();

	  super.open();
     }

     /**
      * Returns the next tuple. If there is a group by field, then the first
      * field is the field by which we are grouping, and the second field is the
      * result of computing the aggregate, If there is no group by field, then
      * the result tuple should contain one field representing the result of the
      * aggregate. Should return null if there are no more tuples.
      */
     protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	  // some code goes here DONE
	  if ( m_aggregates.hasNext() )
	       return m_aggregates.next();

	  return null;
     }

     public void rewind() throws DbException, TransactionAbortedException {
	  // some code goes here DONE
	  m_child.rewind();
	  this.close();
	  this.open();
     }

     /**
      * Returns the TupleDesc of this Aggregate. If there is no group by field,
      * this will have one field - the aggregate column. If there is a group by
      * field, the first field will be the group by field, and the second will be
      * the aggregate value column.
      * 
      * The name of an aggregate column should be informative. For example:
      * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
      * given in the constructor, and child_td is the TupleDesc of the child
      * iterator.
      */
     public TupleDesc getTupleDesc() {
	  // some code goes here DONE
	  return m_td;
     }

     public void close() {
	  // some code goes here DONE
	  m_aggregates.close();
	  super.close();
     }

     @Override
     public DbIterator[] getChildren() {
	  // some code goes here DONE
	  return new DbIterator[] { m_child };
     }

     @Override
     public void setChildren(DbIterator[] children) {
	  // some code goes here
	  m_child = children[0];
     }
    
}
