package com.mt.storage;

import java.util.Map;
import java.util.Set;

public interface Store {
	
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
	 * In case an element is missing (table, row, family, ...), it is created.
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
