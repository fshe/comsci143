package simpledb;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

     private static final long serialVersionUID = 1L;

     private int m_gbfield;
     private Type m_gbfieldtype;
     private int m_afield;
     private Op m_op;
     private HashMap<Object, Integer> m_map;
     private ArrayList<Tuple> m_tuples;

     /**
      * Aggregate constructor
      * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
      * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
      * @param afield the 0-based index of the aggregate field in the tuple
      * @param what aggregation operator to use -- only supports COUNT
      * @throws IllegalArgumentException if what != COUNT
      */

     public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
	  // some code goes here DONE
	  if ( what != Op.COUNT )
	       throw new IllegalArgumentException();

	  m_gbfield = gbfield;
	  m_gbfieldtype = gbfieldtype;
	  m_afield = afield;
	  m_op = what;
	  m_map = new HashMap<Object, Integer>();
	  m_tuples = new ArrayList<Tuple>();
     }

     /**
      * Merge a new tuple into the aggregate, grouping as indicated in the constructor
      * @param tup the Tuple containing an aggregate field and a group-by field
      */
     public void mergeTupleIntoGroup(Tuple tup) {
	  // some code goes here
	  Object group_val;
	  
	  // check the group by field to get appropriate key for map
	  if ( m_gbfield == NO_GROUPING )
	       group_val = null;
	  else 
	  {
	       if ( m_gbfieldtype == Type.INT_TYPE )
		    group_val = ((IntField)tup.getField( m_gbfield )).getValue();
	       else
		    group_val = ((StringField)tup.getField( m_gbfield )).getValue();
	  }

	  // get the value of the aggregate field to be operated on
	  String tup_val = ((StringField)tup.getField( m_afield )).getValue();
	  Integer av = m_map.get( group_val );

	  // create a new object doesn't exist yet
	  if ( av == null )
	       av = 0;

	  // perform appropriate actions base on operation
	  av += 1;

	  // put the value back correlating to it's group, overwriting
	  m_map.put( group_val, av );
     }

     /**
      * Create a DbIterator over group aggregate results.
      *
      * @return a DbIterator whose tuples are the pair (groupVal,
      *   aggregateVal) if using group, or a single (aggregateVal) if no
      *   grouping. The aggregateVal is determined by the type of
      *   aggregate specified in the constructor.
      */
     public DbIterator iterator() {
	  // some code goes here
	  TupleDesc td;
	  Type[] types;
	  String[] field_names;

	  // create appropriate tuple descriptor for grouping or not
	  if ( m_gbfield == NO_GROUPING )
	  {
	       types = new Type[1];
	       types[0] = Type.INT_TYPE;

	       field_names = new String[1];
	       field_names[0] = "aggregateVal";
	  }
	  else
	  {
	       types = new Type[2];
	       types[0] = m_gbfieldtype;
	       types[1] = Type.INT_TYPE;

	       field_names = new String[2];
	       field_names[0] = "groupVal";
	       field_names[1] = "aggregateVal";
	  }

	  // initialize it 
	  td = new TupleDesc( types, field_names );

	  // get a set of all group_vals
	  Set<Object> set = m_map.keySet();
	  Integer av;

	  for ( Object group_val : set )
	  {
	       // grab the aggregate value
	       av = m_map.get( group_val );
	       
	       Tuple t = new Tuple( td );

	       // assign the appropriate field of the tuple
	       if ( m_gbfield == NO_GROUPING )
	       {
		    t.setField( 0, new IntField(av) );
	       }
	       else
	       {
		    // convert the group_val object back to either string or int
		    Field f;
		    if ( m_gbfieldtype == Type.INT_TYPE )
		    {
			 assert( group_val instanceof Integer );
			 f = new IntField( (Integer)group_val );
		    }
		    else
		    {
			 assert( group_val instanceof String );
			 f = new StringField( (String)group_val, m_gbfieldtype.getLen() );
		    }
		    
		    // then insert (groupval, aggregateval) as a tuple
		    t.setField( 0, f );
		    t.setField( 1, new IntField(av) );
	       }

	       m_tuples.add( t );
	  }

	  return new TupleIterator( td, m_tuples );
     }

}
