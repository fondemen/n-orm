package com.googlecode.n_orm.storeapi;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;

public interface TypeAwareStore {

	public abstract void start() throws DatabaseNotReachedException;

	public abstract void delete(PersistingElement elt, String table, String id)
			throws DatabaseNotReachedException;

	public abstract boolean exists(PersistingElement elt, String table,
			String row) throws DatabaseNotReachedException;

	public abstract boolean exists(PersistingElement elt, Field columnFamily,
			String table, String row, String family)
			throws DatabaseNotReachedException;

	public abstract CloseableKeyIterator get(
			Class<? extends PersistingElement> type, Set<Field> columnFamilies,
			String table, Constraint c, int limit, Set<String> families)
			throws DatabaseNotReachedException;

	public abstract byte[] get(PersistingElement elt, Field property,
			String table, String row, String family, String key)
			throws DatabaseNotReachedException;

	public abstract Map<String, byte[]> get(PersistingElement elt,
			Field columnFamily, String table, String id, String family)
			throws DatabaseNotReachedException;

	public abstract Map<String, byte[]> get(PersistingElement elt,
			Field columnFamily, String table, String id, String family,
			Constraint c) throws DatabaseNotReachedException;

	public abstract Map<String, Map<String, byte[]>> get(PersistingElement elt,
			String table, String id, Set<String> families)
			throws DatabaseNotReachedException;

	public abstract void storeChanges(PersistingElement elt, String table,
			String id, Map<String, Map<String, byte[]>> changed,
			Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> increments)
			throws DatabaseNotReachedException;

	public abstract long count(Class<? extends PersistingElement> type,
			String table, Constraint c) throws DatabaseNotReachedException;

}