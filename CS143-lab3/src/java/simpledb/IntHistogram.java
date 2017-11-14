package simpledb;
import java.lang.Math;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

     private int[] m_bucket_arr;
     private int m_nbuckets;
     private int m_min;
     private int m_max;
     private int m_count;
     private int m_w;

     /**
      * Create a new IntHistogram.
      * 
      * This IntHistogram should maintain a histogram of integer values that it receives.
      * It should split the histogram into "buckets" buckets.
      * 
      * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
      * 
      * Your implementation should use space and have execution time that are both
      * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
      * simply store every value that you see in a sorted list.
      * 
      * @param buckets The number of buckets to split the input value into.
      * @param min The minimum integer value that will ever be passed to this class for histogramming
      * @param max The maximum integer value that will ever be passed to this class for histogramming
      */
     public IntHistogram(int buckets, int min, int max) {
	  // some code goes here DONE
	  m_bucket_arr = new int[buckets];
	  for ( int i = 0; i < buckets; i++ )
	       m_bucket_arr[i] = 0;

	  m_nbuckets = buckets;
	  m_min = min;
	  m_max = max;
	  m_count = 0;

	  m_w = (int)Math.ceil( (double)(max-min+1)/(double)(buckets) );
     }

     /**
      * Add a value to the set of values that you are keeping a histogram of.
      * @param v Value to add to the histogram
      */
     public void addValue(int v) {
	  // some code goes here DONE
	  m_count++;

	  int bucket = (v-m_min)/m_w;
	  m_bucket_arr[bucket]++;
     }

     /**
      * Estimate the selectivity of a particular predicate and operand on this table.
      * 
      * For example, if "op" is "GREATER_THAN" and "v" is 5, 
      * return your estimate of the fraction of elements that are greater than 5.
      * 
      * @param op Operator
      * @param v Value
      * @return Predicted selectivity of this particular operator and value
      */
     public double estimateSelectivity(Predicate.Op op, int v) {

	  // some code goes here
	  double result = 0.0;
	  int bucket = 0;

	  if ( m_min < v && v < m_max )
	       bucket = (v-m_min)/m_w;

	  double h = m_bucket_arr[bucket];
	  double w = (double)m_w;
	  double ntups = (double)m_count;

	  double b_right = (double)(bucket+1) * w;
	  double b_left = (double)bucket * w;
	  double b_f = h/ntups;
	  double b_part = 0.0;

	  if ( op == Predicate.Op.EQUALS )
	  {
	       if ( v < m_min || m_max < v )
		    return 0;
	       result = (h/w) / ntups;
	  }
	  else if ( op == Predicate.Op.NOT_EQUALS )
	  {
	       if ( v < m_min || m_max < v )
		    return 1;
	       result = (ntups-(h/w)) / ntups;
	  }
	  else if ( op == Predicate.Op.GREATER_THAN || 
		    op == Predicate.Op.GREATER_THAN_OR_EQ )
	  {
	       if ( op == Predicate.Op.GREATER_THAN_OR_EQ )
	       {
		    if ( v >= m_max )
			 return 0.0;
		    if ( v <= m_min )
			 return 1.0;
	       }
	       else
	       {
		    if ( v > m_max )
			 return 0.0;
		    if ( v < m_min )
			 return 1.0;
	       }

	       for ( int i = bucket+1; i < m_nbuckets; i++ )
		    result += (double)m_bucket_arr[i]/ntups;

	       if ( op == Predicate.Op.GREATER_THAN )
		    b_part  = (b_right - v)/w;
	       else
		    b_part  = (b_right + 1 - v)/w;
	       
	       result += b_f * b_part;
	  }
	  else if ( op == Predicate.Op.LESS_THAN ||
		    op == Predicate.Op.LESS_THAN_OR_EQ )
	  {
	       if ( v > m_max )
		    return 1.0;
	       if ( v < m_min )
		    return 0.0;

	       for ( int i = bucket-1; i >= 0; i-- )
		    result += (double)m_bucket_arr[i]/ntups;

	       if ( op == Predicate.Op.LESS_THAN )
		    b_part  = (v - b_left)/w;
	       else
		    b_part  = (v + 1 - b_left)/w;
	       
	       result += b_f * b_part;
	  }

	  return result;
     }
    
     /**
      * @return
      *     the average selectivity of this histogram.
      *     
      *     This is not an indispensable method to implement the basic
      *     join optimization. It may be needed if you want to
      *     implement a more efficient optimization
      * */
     public double avgSelectivity()
	  {
	       // some code goes here
	       return 1.0;
	  }
    
     /**
      * @return A string describing this histogram, for debugging purposes
      */
     public String toString() {
	  // some code goes here DONE
	  String result = "";

	  result += "Min = " + m_min + "\n";
	  result += "Max = " + m_max + "\n";
	  result += "Number of Buckets = " + m_nbuckets + "\n";
	  result += "Bucket Width = " + m_w + "\n";
	  result += "Ntuples = " + m_count + "\n";
	  
	  for ( int i = 0; i < m_nbuckets; i++ )
	  {
	       result += "Bucket #" + i + " has " + 
		    m_bucket_arr + " tuples\n";
	  }
	  
	  return result;
     }
}
