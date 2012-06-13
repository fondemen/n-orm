package com.googlecode.n_orm.storeapi;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;

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
	public void start() throws DatabaseNotReachedException;
	
	/**
	 * Checks whether the given table exists in this store.
	 */
	public boolean hasTable(String tableName) throws DatabaseNotReachedException;
	
	/**
	 * Tests whether a column family is empty.
	 */
	public void delete(PersistingElement elt, String table, String id)
			throws DatabaseNotReachedException;
	/**
	 * Tests for a row.
	 */
	public boolean exists(PersistingElement elt, String table,
			String row) throws DatabaseNotReachedException;

	/**
	 * Tests whether a column family is empty.
	 */
	public boolean exists(PersistingElement elt, Field columnFamily,
			String table, String row, String family)
			throws DatabaseNotReachedException;

	/**
	 * Rows matching constraint sorted according to their key in ascending order.
	 */
	public CloseableKeyIterator get(
			Class<? extends PersistingElement> type,
			String table, Constraint c, int limit, Map<String, Field> families)
			throws DatabaseNotReachedException;
	
	/**
	 * Returns an element from a family.
	 */
	public byte[] get(PersistingElement elt, Field property,
			String table, String row, String family, String key)
			throws DatabaseNotReachedException;

	/**
	 * Returns all elements in a family ; no side-effect.
	 * In case one element is missing, null is returned.
	 */
	public Map<String, byte[]> get(PersistingElement elt,
			Field columnFamily, String table, String id, String family)
			throws DatabaseNotReachedException;

	/**
	 * Returns all elements in a family ; no side-effect.
	 * In case one element is missing, null is returned.
	 */
	public Map<String, byte[]> get(PersistingElement elt,
			Field columnFamily, String table, String id, String family,
			Constraint c) throws DatabaseNotReachedException;

	/**
	 * Returns all elements in families ; no side-effect.
	 * In case one element is missing, null is returned.
	 */
	public Map<String, Map<String, byte[]>> get(PersistingElement elt,
			String table, String id,
			Map<String, Field> families) throws DatabaseNotReachedException;
	/**
	 * Stores given piece of information.
	 * In case an element is missing in the data store (table, row, family, ...), it is created.
	 */
	public void storeChanges(PersistingElement elt,
			Map<String, Field> changedFields, String table, String id,
			Map<String, Map<String, byte[]>> changed,
			Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> increments)
			throws DatabaseNotReachedException;

	/**
	 * Counts the number of element satisfying the constraint.
	 */
	public long count(Class<? extends PersistingElement> type,
			String table, Constraint c) throws DatabaseNotReachedException;

}