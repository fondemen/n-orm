package com.googlecode.n_orm.storeapi;

import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;

/**
 * The interface that defines what a data store should implement. The expected
 * data model is that one of column-oriented databases such as data models of <a
 * href="http://labs.google.com/papers/bigtable.html">BigTable</a>, <a
 * href="http://wiki.apache.org/hadoop/Hbase/DataModel">HBase</a> or <a
 * href="http://wiki.apache.org/cassandra/DataModel">Cassandra</a>. To
 * summarize, a data store should provide a set of maps (we'll call
 * <i>tables</i>). Each one of these <b>tables</b> is a map which contains
 * <i>rows</i> that associate a {@link String} <i>key</i> with a set of
 * <i>column families</i>. It should be easy and efficient to find a row in a
 * table using its key, or defining a key with a minimum value and a maximum
 * value as if key would be organized in a dictionary (see
 * {@link String#compareTo(String)}). Each <b>column family</b> is a map which
 * associates a <i>qualifier</i> with data, represented by a byte array.
 * 
 * @see com.googlecode.n_orm.memory.Memory a default implementation
 */
public interface Store {

	/**
	 * Called once the store is created ; only one store is instanciated with
	 * the same properties.
	 */
	public void start() throws DatabaseNotReachedException;

	/**
	 * Checks whether the given table exists in this store.
	 */
	public boolean hasTable(String tableName)
			throws DatabaseNotReachedException;

	/**
	 * Deletes a particular element.
	 */
	public void delete(MetaInformation meta, String table, String id)
			throws DatabaseNotReachedException;

	/**
	 * Tests for a row.
	 */
	public boolean exists(MetaInformation meta, String table, String row)
			throws DatabaseNotReachedException;

	/**
	 * Tests whether a column family is empty.
	 */
	public boolean exists(MetaInformation meta, String table, String row,
			String family) throws DatabaseNotReachedException;

	/**
	 * Rows matching constraint sorted according to their key in ascending
	 * order.
	 */
	public CloseableKeyIterator get(MetaInformation meta, String table,
			Constraint c, int limit, Set<String> families)
			throws DatabaseNotReachedException;

	/**
	 * Returns an element from a family.
	 */
	public byte[] get(MetaInformation meta, String table, String row,
			String family, String key) throws DatabaseNotReachedException;

	/**
	 * Returns all elements in a family ; no side-effect. In case one element is
	 * missing, null is returned.
	 */
	public Map<String, byte[]> get(MetaInformation meta,
			String table, String id, String family)
			throws DatabaseNotReachedException;

	/**
	 * Returns all elements in a family ; no side-effect. In case one element is
	 * missing, null is returned.
	 */
	public Map<String, byte[]> get(MetaInformation meta,
			String table, String id, String family, Constraint c)
			throws DatabaseNotReachedException;

	/**
	 * Returns all elements in families ; no side-effect. In case element with
	 * the given key is missing, null is returned.
	 * 
	 * @param table
	 *            the table from which to find the element
	 * @param id
	 *            the unique identifier (i.e. the key) with which the element
	 *            was stored
	 * @param families
	 *            the set of column families to be activated ; should never be
	 *            null or empty
	 * @return the data stored for each family ; null if and only if the id does
	 *         not exist within the given table
	 */
	public ColumnFamilyData get(MetaInformation meta, String table, String id,
			Set<String> families) throws DatabaseNotReachedException;

	/**
	 * Stores given piece of information. In case an element is missing in the
	 * data store (table, row, family, ...), it is created.
	 */
	public void storeChanges(MetaInformation meta, String table, String id,
			ColumnFamilyData changed, Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> increments)
			throws DatabaseNotReachedException;

	/**
	 * Counts the number of element satisfying the constraint.
	 */
	public long count(MetaInformation meta, String table,
			Constraint c) throws DatabaseNotReachedException;

}