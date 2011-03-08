package com.mt.storage;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;
import java.util.Set;

import com.mt.storage.PropertyManagement.PropertyFamily;
import com.mt.storage.cf.ColumnFamily;

public interface PersistingElement extends Comparable<PersistingElement>, Serializable {
	static final Class<?>[] PossiblePropertyTypes = new Class[] { Date.class,
			String.class, Boolean.class, int.class, byte.class, short.class,
			long.class, float.class, double.class, boolean.class,
			char.class, Integer.class, Byte.class, Short.class, Long.class,
			Float.class, Double.class, Boolean.class,
			Character.class };

	Store getStore();

	String getTable();

	List<Field> getKeys();
	PropertyFamily getProperties();
	Set<ColumnFamily<?>> getColumnFamilies();
	ColumnFamily<?> getColumnFamily(String columnFailyName);

	String getIdentifier();

	boolean hasChanged();

	void store() throws DatabaseNotReachedException;
	void delete() throws DatabaseNotReachedException;
	boolean existsInStore() throws DatabaseNotReachedException;

	void activate(String... families) throws DatabaseNotReachedException;
}
