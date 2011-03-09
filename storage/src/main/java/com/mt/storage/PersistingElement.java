package com.mt.storage;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;
import java.util.Set;

import com.mt.storage.PropertyManagement.PropertyFamily;
import com.mt.storage.cf.ColumnFamily;

/**
 * Persisting elements are elements that can be stored and retrieved from a {@link Store}.
 * To make a class persisting, do not implement this interface, but rather declare the annotation {@link Persisting}.
 * @see Persisting
 */
public interface PersistingElement extends Comparable<PersistingElement>, Serializable {
	/**
	 * The list of possible types for simple properties (including keys).
	 * Other possible types are array of a possible type, persisting elements and classes with only keys.
	 */
	static final Class<?>[] PossiblePropertyTypes = new Class[] { Date.class,
			String.class, Boolean.class, int.class, byte.class, short.class,
			long.class, float.class, double.class, boolean.class,
			char.class, Integer.class, Byte.class, Short.class, Long.class,
			Float.class, Double.class, Boolean.class,
			Character.class };

	/**
	 * The store used for this persisting element.
	 */
	Store getStore();

	/**
	 * The table used to store this persisting element as declared by the {@link Persisiting#table()} annotation.
	 * If the persisting element inherits another persisting element, only the table for the instanciated class is shown.
	 */
	String getTable();

	/**
	 * The list of keys for this persisting element.
	 * A key is a property annotated with {@link Key}.
	 * The result is sorted in the order declared by {@link Key#order()}.
	 */
	List<Field> getKeys();
	
	/**
	 * The {@link ColumnFamily} used to store properties.
	 * A property is a non static, non final, non transient field whose type is one of {@link #PossiblePropertyTypes} plus persisting elements plus classes with only key properties and no column family, plus arrays of such types.
	 * Keys are also readable from this family.
	 * The values stored in this column families are the last activated.
	 * @see #activate(String...)
	 */
	PropertyFamily getProperties();
	
	/**
	 * The list of all {@link ColumnFamily} held by this persisting element.
	 */
	Set<ColumnFamily<?>> getColumnFamilies();
	
	/**
	 * The the {@link ColumnFamily} held by this persisting element with the given name.
	 * The name is the name of the property for the column family, i.e. the non static, non final , non transient {@link Map} or {@link Set} field.
	 * @see ColumnFamily#getName()
	 */
	ColumnFamily<?> getColumnFamily(String columnFailyName);

	/**
	 * The identifier for this persisting element.
	 * The identifier is computed from a string representation of all keys of this persisting element separated with {@link KeyManagement#KEY_SEPARATOR}.
	 * Moreover, the identifier ends with {@link KeyManagement#KEY_END_SEPARATOR}.
	 * @throws IllegalStateException in case all keys have not been set
	 * @see com.mt.storage.conversion.ConversionTools#convertToString(Object)
	 */
	String getIdentifier();
	
	/**
	 * Same result as {@link #getIdentifier()}, but postfixed with the name of the class instanciated by this persisting element.
	 * @see Object#getClass()
	 */
	String getFullIdentifier();

	/**
	 * Checks whether this persisting has changed since its last activation.
	 * In case this object has not changed, a {@link #store()} will not trigger any data store request.
	 * @see #activate(String...)
	 */
	boolean hasChanged();

	/**
	 * Stores the object into the data store designated by {@link #getStore()}.
	 * All non static final or transient fields will be stored into the datastore.
	 * All keys have to be set before invoking this operation
	 * <p>This object is stored as a row in table designated by {@link #getTable()} using the key computed from {@link #getIdentifier()}.
	 * Properties are stored in a column family whose name is given by {@link PropertyManagement#PROPERTY_COLUMNFAMILY_NAME}.
	 * Column families are stored using their own name (see {@link #getColumnFamily(String)}), that is the name of the {@link Map} or {@link Set} fields.
	 * Properties and column families which have not changed since last activation of this element are not sent to the store as they are supposed to be already stored.
	 * </p><p>In case this persisting element inherits another persisting element class, a row with the full identifier is created in the tables for the ancestor class (see {@link Persisting#table()}).
	 * Moreover, a column named {@link StorageManagement#CLASS_COLUMN} within a column family named {@link StorageManagement#CLASS_COLUMN_FAMILY} stores the name of the actual class of this persisting element in those tables for superclasses.
	 * Properties and column families are not stored in those rows unless stated by the {@link Persisting#storeAlsoInSuperClasses()} annotation in the actual class of this persisting element.
	 * </p><p>To choose a store, you need to supply in the classpath a properties file depending on the nature of the data store you want to use.
	 * This property file is to be found in the class path. For a persisting element of a class in package foo.bar, the property file is searched as foo/bar/store.propetries in the complete classpath (in the declared order), then as foo/store.properties, and then directly store.properties.
	 * To know how to define the properties file, please refer to your data store supplier. An (naive) example is the {@link com.mt.storage.memory.Memory} data store.
	 * </p>
	 * @throws DatabaseNotReachedException in case the store cannot store this persisting object (e.g. cannot connect to database)
	 * @see #getIdentifier()
	 * @see Store
	 */
	void store() throws DatabaseNotReachedException;
	
	/**
	 * Deletes rows representing this persisting element in the store.
	 * @see #store()
	 */
	void delete() throws DatabaseNotReachedException;
	
	/**
	 * Checks whether the row representing this persisting element exists in the store.
	 * Rows used to store this element in table  for its super-classes are not tested.
	 * @see #store()
	 */
	boolean existsInStore() throws DatabaseNotReachedException;

	/**
	 * Retrieves information from the store and put it into this persisting element.
	 * Properties that are persisting elements are also activated.
	 * Column families annotated with {@link ImplicitActivation} are also activated.
	 * @param families names of the families to activate even if they are not annotated with {@link ImplicitActivation} (see {@link #getColumnFamily(String)})
	 * @throws IllegalArgumentException in case a given column family does not exists
	 */
	void activate(String... families) throws DatabaseNotReachedException;
	
	/**
	 * To be equal, two persisting elements must implement the same class and have the same identifier, no matter they have same values for properties and column families.
	 * @see Object#equals(Object)
	 */
	boolean equals(Object rhs);
	
	/**
	 * The hash code for {@link #getIdentifier()}.
	 * This method make possible to use persisting elements in {@link java.util.Hashtable}s, {@link java.util.HashMap}s or  {@link java.util.HashSet}s.
	 * @see Object#hashCode()
	 */
	int hashCode();
	
	/**
	 * If instanciated classes are different, class names are compared, in the other case, the result of {@link #getIdentifier()} are compared.
	 * This method make possible to use persisting elements in {@link java.util.TreeMap}s or  {@link java.util.TreeSet}s.
	 * @see Comparable#compareTo(Object)
	 */
	int compareTo(PersistingElement rhs);
}