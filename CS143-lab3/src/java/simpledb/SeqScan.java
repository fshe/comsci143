package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

     private static final long serialVersionUID = 1L;
     private String tableAlias;
     private int tableid;
     private DbFileIterator iter;
     private TransactionId tid;
     /**
      * Creates a sequential scan over the specified table as a part of the
      * specified transaction.
      * 
      * @param tid
      *            The transaction this scan is running as a part of.
      * @param tableid
      *            the table to scan.
      * @param tableAlias
      *            the alias of this table (needed by the parser); the returned
      *            tupleDesc should have fields with name tableAlias.fieldName
      *            (note: this class is not responsible for handling a case where
      *            tableAlias or fieldName are null. It shouldn't crash if they
      *            are, but the resulting name can be null.fieldName,
      *            tableAlias.null, or null.null).
      */
     public SeqScan(TransactionId tid, int tableid, String tableAlias) {
	  this.tableAlias = tableAlias;
	  this.tid = tid;
	  this.tableid = tableid;

     }

     /**
      * @return
      *       return the table name of the table the operator scans. This should
      *       be the actual name of the table in the catalog of the database
      * */
     public String getTableName() {
	  return Database.getCatalog().getTableName(tableid);
     }
    
     /**
      * @return Return the alias of the table this operator scans. 
      * */
     public String getAlias()
	  {
	       return tableAlias;
	  }

     /**
      * Reset the tableid, and tableAlias of this operator.
      * @param tableid
      *            the table to scan.
      * @param tableAlias
      *            the alias of this table (needed by the parser); the returned
      *            tupleDesc should have fields with name tableAlias.fieldName
      *            (note: this class is not responsible for handling a case where
      *            tableAlias or fieldName are null. It shouldn't crash if they
      *            are, but the resulting name can be null.fieldName,
      *            tableAlias.null, or null.null).
      */
     public void reset(int tableid, String tableAlias) {
	  this.tableid = tableid;
	  this.tableAlias = tableAlias;
     }

     public SeqScan(TransactionId tid, int tableid) {
	  this(tid, tableid, Database.getCatalog().getTableName(tableid));
     }

     public void open() throws DbException, TransactionAbortedException {
	  DbFile f = Database.getCatalog().getDatabaseFile(tableid);
	  iter = f.iterator(tid);
	  iter.open();
     }

     /**
      * Returns the TupleDesc with field names from the underlying HeapFile,
      * prefixed with the tableAlias string from the constructor. This prefix
      * becomes useful when joining tables containing a field(s) with the same
      * name.
      * 
      * @return the TupleDesc with field names from the underlying HeapFile,
      *         prefixed with the tableAlias string from the constructor.
      */
     public TupleDesc getTupleDesc() {
	  DbFile f = Database.getCatalog().getDatabaseFile(tableid); //get the file
	  TupleDesc td = f.getTupleDesc();
	  int size = td.numFields();
	  Type[] types = new Type[size];
	  String[] strings = new String[size];
	  for(int i = 0; i < size; i++)
	  {
	       types[i] = td.getFieldType(i);
	       strings[i] = tableAlias + "." + td.getFieldName(i);
	  }
	
	  TupleDesc returnVal = new TupleDesc(types, strings);
	  return returnVal;
     }

     public boolean hasNext() throws TransactionAbortedException, DbException {
	  if ( iter == null )
	       return false;
	  return iter.hasNext();
     }

     public Tuple next() throws NoSuchElementException,
	  TransactionAbortedException, DbException {
	  if ( iter == null || !iter.hasNext() )
	       throw new NoSuchElementException();
	  return iter.next();
     }

     public void close() {
	  iter.close();
     }

     public void rewind() throws DbException, NoSuchElementException,
	  TransactionAbortedException {
	  iter.rewind();
     }
}
