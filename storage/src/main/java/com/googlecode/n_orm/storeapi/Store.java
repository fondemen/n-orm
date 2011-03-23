package com.googlecode.n_orm.storeapi;

import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.CloseableKeyIterator;
import com.googlecode.n_orm.DatabaseNotReachedException;

/**
 * The interface that defines what a data store should implement.
 * The expected data model is that one of column-oriented databases such as data models of <a href="http://labs.google.com/papers/bigtable.html">BigTable</a>, <a href="http://wiki.apache.org/hadoop/Hbase/DataModel">HBase</a> or <a href="http://wiki.apache.org/cassandra/DataModel">Cassandra</a>.
 * To summarize, a data store should provide a set of maps (we'll call <i>tables</i>).
 * Each one of these <b>tables</b> is a map which contains <i>rows</i> that associate a {@link String} <i>key</i> with a set of <i>column families</i>.
 * It should be easy and efficient to find a row in a table using its key, or defining a key with a minimum value and a maximum value as if key would be organized in a dictionary (see {@link String#compareTo(String)}).
 * Each <b>column family</b> is a map which associates a <i>qualifier</i> with data, represented by a byte array.
 * @see com.googlecode.n_orm.memory.Memory a default implementation
 */
public interface Store {
	
	/**
	 * Called once the store is created ; only one store is instanciated with the same properties.
	 */
	void start() throws DatabaseNotReachedException;
	
	//void add(String table, String id, String family, String key, byte[] value);
	//void remove(String table, String id, String family, String key);
	
	/**
	 * Tests for a row.
	 */
	boolean exists(String table, String row) throws DatabaseNotReachedException;
	
	/**
	 * Tests whether a column family is empty.
	 */
	boolean exists(String table, String row, String family) throws DatabaseNotReachedException;
	
//	/**
//	 * Tests for a key.
//	 */
//	boolean exists(String table, String row, String family, String key) throws DatabaseNotReachedException;
	
	/**
	 * Rows matching constraint sorted according to their key in ascending order.
	 */
	CloseableKeyIterator get(String table, Constraint c, int limit) throws DatabaseNotReachedException;
	
	/**
	 * Returns an element from a family.
	 */
	byte [] get(String table, String row, String family, String key) throws DatabaseNotReachedException;
	
	/**
	 * Returns all elements in a family ; no side-effect.
	 * In case one element is missing, null is returned.
	 */
	Map<String, byte[]> get(String table, String id, String family) throws DatabaseNotReachedException;
	
	/**
	 * Returns all elements in a family ; no side-effect.
	 * In case one element is missing, null is returned.
	 */
	Map<String, byte[]> get(String table, String id, String family, Constraint c) throws DatabaseNotReachedException;
	
	/**
	 * Returns all elements in families ; no side-effect.
	 * In case one element is missing, null is returned.
	 */
	Map<String, Map<String, byte[]>> get(String table, String id, Set<String> families) throws DatabaseNotReachedException;
	
	/**
	 * Stores given piece of information.
	 * In case an element is missing in the data store (table, row, family, ...), it is created.
	 */
	void storeChanges(String table, String id, Map<String, Map<String, byte[]>> changed, Map<String, Set<String>> removed, Map<String, Map<String, Number>> increments) throws DatabaseNotReachedException;
	
	/**
	 * Deletes the given row
	 */
	void delete(String table, String id) throws DatabaseNotReachedException;
	
//	/**
//	 * Counts the number of elements in the table.
//	 * In case the table is missing, 0 is returned.
//	 */
//	int count(String table) throws DatabaseNotReachedException;

//	/**
//	 * Counts the number of elements in the family.
//	 * In case the an element is missing (table, row, family, ...), 0 is returned.
//	 */
//	int count(String table, String row, String family) throws DatabaseNotReachedException;


//	/**
//	 * Counts the number of elements satisfying a constraint in the family.
//	 * In case the an element is missing (table, row, family, ...), 0 is returned.
//	 */
//	int count(String ownerTable, String identifier, String name,
//			Constraint constraint) throws DatabaseNotReachedException;
}
