package com.mt.storage;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;

public interface PersistingElement {
	static final Class<?>[] PossiblePropertyTypes = new Class[] { Date.class,
			String.class, Boolean.class, int.class, byte.class, short.class,
			long.class, float.class, double.class, boolean.class,
			char.class, Integer.class, Byte.class, Short.class, Long.class,
			Float.class, Double.class, Boolean.class,
			Character.class };

	Store getStore();

	String getTable();

	List<Field> getKeys();

	String getIdentifier();

	boolean hasChanged();

	void store() throws DatabaseNotReachedException;
	void delete() throws DatabaseNotReachedException;
	boolean existsInStore() throws DatabaseNotReachedException;

	void activate(String... families) throws DatabaseNotReachedException;
}
