package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private int m_tableid;
    private int m_ioCostPerPage;
    private HeapFile file;
    private TupleDesc td;
    private int nFields;
    private int nTups;
    private HashMap<String, Integer> minVals;
    private HashMap<String, Integer> maxVals;
    private HashMap<String, Object> histList;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
	nTups = 0;
	m_tableid = tableid;
	m_ioCostPerPage = ioCostPerPage;
	file = (HeapFile) Database.getCatalog().getDbFile(tableid);

	Transaction trans = new Transaction();
	TransactionId tid = trans.getId();
	DbFileIterator iter = file.iterator(tid);
	td = file.getTupleDesc();
	nFields = td.numFields();

	minVals = new HashMap<String, Integer>();
	maxVals = new HashMap<String, Integer>();
	histList = new HashMap<String, Object>();
	
	try
	{
	iter.open();
	while(iter.hasNext())
	{
		Tuple temp = iter.next();
		for ( int i = 0; i < nFields; i++)
		{
			String name = td.getFieldName(i);
			if(td.getFieldType(i).equals(Type.INT_TYPE)) //Integer Field
			{
				int value = ((IntField)temp.getField(i)).getValue();
				if(!minVals.containsKey(name) || value < minVals.get(name))
					minVals.put(name, value);
				if(!maxVals.containsKey(name) || value > maxVals.get(name))
					maxVals.put(name, value);
			}
		}
		
	}
	for( String key : maxVals.keySet() )
	{
		IntHistogram inth = new IntHistogram(NUM_HIST_BINS, minVals.get(key), maxVals.get(key));
		histList.put(key, inth);
	}
	
	iter.rewind();
	while(iter.hasNext()) //add integer type histograms
	{
		nTups++;
		Tuple temp = iter.next();
		for (int i = 0; i < nFields; i++)
		{
			Type type = td.getFieldType(i);
			String name = td.getFieldName(i);
			if(type.equals(Type.INT_TYPE))
			{
				int val = ((IntField)temp.getField(i)).getValue();
				IntHistogram inth = (IntHistogram) histList.get(name);
				inth.addValue(val);
				histList.put(name, inth);
			}
			else
			{
                                String val = ((StringField)temp.getField(i)).getValue();
                                StringHistogram stringh = new StringHistogram(NUM_HIST_BINS);
                                stringh.addValue(val);
                                histList.put(name, stringh);
                        }
		}
	}
	} catch (DbException exception) {
	}catch (TransactionAbortedException exception2 ) {}
	
	
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
	return file.numPages()*m_ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
	return (int) Math.ceil(nTups*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
	String name = td.getFieldName(field);
	switch(constant.getType())
	{
		case INT_TYPE:
		{
			int value = ((IntField) constant).getValue();
			IntHistogram inth = (IntHistogram) histList.get(name);
			return inth.estimateSelectivity(op, value);
		}
		case STRING_TYPE:
		{
			String value = ((StringField) constant).getValue();
			StringHistogram stringh = (StringHistogram) histList.get(name);
			return stringh.estimateSelectivity(op, value);
		}
		default:
			break;
	}
	return -1;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
	return nTups;
    }

}
