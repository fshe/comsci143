package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> TD_array;  //dragon
    private int size; //dragon

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.TD_array.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
	this.size = 0; //dragon check if there is at least one entry
	this.TD_array = new ArrayList<TDItem>(typeAr.length);
        for(int i = 0; i < typeAr.length; i++)
	{
		this.TD_array.add(new TDItem(typeAr[i], fieldAr[i]));
		this.size += typeAr[i].getLen(); //dragon
	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
	this.size = 0; //dragon
        this.TD_array = new ArrayList<TDItem>(typeAr.length);
        for(int i = 0; i < typeAr.length; i++)
        {
                this.TD_array.add(new TDItem(typeAr[i], null));
		this.size += typeAr[i].getLen(); //dragon
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.TD_array.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
	if (i < this.TD_array.size())
		return this.TD_array.get(i).fieldName;
	else
		throw new NoSuchElementException();	
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < this.TD_array.size())
                return this.TD_array.get(i).fieldType;
        else
                throw new NoSuchElementException();
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
	if (name == null)
		throw new NoSuchElementException();
	Iterator<TDItem> iter = this.iterator();
	for(int i = 0; iter.hasNext(); i++)
	{
		TDItem temp = iter.next();
		if( temp.fieldName == null)
			throw new NoSuchElementException();
		if(temp.fieldName.equals(name))
			return i;
	}
	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return this.size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */

    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {  
	// some code goes here DONE
	int array_size = td1.numFields() + td2.numFields(); 
	Type[] type_array = new Type[array_size]; 
	String[] string_array = new String[array_size]; 
	int i; 
	Iterator<TDItem> td1_iter = td1.iterator(); 
	Iterator<TDItem> td2_iter = td2.iterator();
	for( i = 0; td1_iter.hasNext(); i++ ) 
	{ 
		TDItem temp = td1_iter.next(); 
		type_array[i] = temp.fieldType; 
		string_array[i] = temp.fieldName; 
	}

	for( i = td1.numFields(); td2_iter.hasNext(); i++ ) 
	{ 
		TDItem temp = td2_iter.next(); 
		type_array[i] = temp.fieldType; 
		string_array[i] = temp.fieldName; 
	}
	return new TupleDesc( type_array, string_array ); 
}

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
	//dragon need code to check that o is a TupleDesc
	if (o == null) return false;
	try
	{
	TupleDesc temp = (TupleDesc) o;
	}
	catch (ClassCastException exception)
	{
		return false;
	}
	TupleDesc temp = (TupleDesc) o;	
	if( this.getSize() != temp.getSize() || this.numFields() != temp.numFields() )
		return false;
	Iterator<TDItem> iter = this.iterator();
	Iterator<TDItem> iter2 = temp.iterator();
	while( iter.hasNext() )
	{
		if(!iter2.hasNext()) //dragon this is unnecessary
			return false;
		if(iter.next().fieldType != iter2.next().fieldType)
			return false;
	}	
	//dragon should be fine not to check if iter2 still has next lulz
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
	String returnVal = "";
	Iterator<TDItem> iter = this.iterator();
	int first = 0;
	while( iter.hasNext() )
	{
		if (first != 0)
			returnVal += ", ";
		first = 1;
		TDItem current = iter.next();
		returnVal += current.fieldType + "(" + current.fieldName + ")";
	}
	
        return returnVal;
    }
}
