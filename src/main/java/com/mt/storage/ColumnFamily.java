package com.mt.storage;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.mt.storage.conversion.ConversionTools;

public class ColumnFamily<T> implements Collection<T> {
	public static enum ChangeKind {SET, DELETE};
	
	private final Class<T> clazz;
	private final String name;
	private final PersistingElement owner;
	private final String ownerTable;
	private final Field index;
	private final Store store;
	private final boolean addOnly;
	private final Field incrementingField;
	private boolean activated = false;

	private final Map<String, T> collection = new TreeMap<String, T>();

	private final Map<String, ChangeKind> changes;
	private final Map<String, Number> increments;

	public ColumnFamily(Class<T> clazz, String name, PersistingElement owner, String index, boolean addOnly, boolean incremental) throws SecurityException, NoSuchFieldException {
		this(clazz, name, owner, clazz.getField(index), addOnly, incremental);
	}

	public ColumnFamily(Class<T> clazz, String name, PersistingElement owner, Field index, boolean addOnly, boolean incremental) {
		super();
		this.clazz = clazz;
		this.name = name;
		this.owner = owner;
		this.index = index;
		this.addOnly = addOnly;
		this.store = this.owner.getStore();
		this.ownerTable = this.owner.getTable();
		if (incremental) {
			if (PersistingElement.class.isAssignableFrom(clazz))
				throw new IllegalArgumentException("Persisting elements such as " + clazz + " cannot be set as incrementing in collection " + name);
			this.increments = new TreeMap<String, Number>();
			this.changes = null;
			Field incf = null;
			for (Field field : PropertyManagement.aspectOf().getProperties(clazz)) {
				if (!field.equals(index)) {
					if (incf == null)
						incf = field;
					else
						throw new IllegalArgumentException("An incrementing collection must contain elements of only two fields: an index and a value, whic is not the case of " + clazz);
				}
			}
			IncrementManagement.aspectOf().checkIncrementable(incf.getType());
			this.incrementingField = incf;
		} else {
			this.changes = new TreeMap<String, ChangeKind>();
			this.increments = null;
			this.incrementingField = null;
		}
	}
	
	public String getName() {
		return name;
	}

	public PersistingElement getOwner() {
		return owner;
	}

	public boolean isAddOnly() {
		return addOnly || this.increments != null;
	}

	protected String getIndex(T object) {
		return PropertyManagement.aspectOf().candideReadValue(object, this.index).toString();
	}
	
	protected boolean wasActivated() {
		return this.activated;
	}
	
	public void activate(Object startIndex, Object endIndex) throws DatabaseNotReachedException {
		this.activate(startIndex == null && endIndex == null ? null : new Constraint(null, this.index, startIndex, endIndex, false));
	}
	
	public void activate(Constraint c) throws DatabaseNotReachedException {
		if (!this.activated) {
			reactivate(c);
		}
	}

	public void reactivate(Constraint c) throws DatabaseNotReachedException {
		this.collection.clear();
		this.clearChanges();
		String id = this.owner.getIdentifier();
		assert id != null;
		Map<String, byte[]> elements = c == null ? this.store.get(this.ownerTable, id, this.name) : this.store.get(this.ownerTable, id, this.name, c);
		for (String key : elements.keySet()) {
			if (this.incrementingField != null) { //Then we must have two keys: identifier (key) and number (in elements)
				//Value is in byte ; to get the actual value with ConversionTools.convert, we need to create the expected ID
				Number eltVal = (Number) ConversionTools.convert(this.incrementingField.getType(), elements.get(key));
				String eltRep = ConversionTools.convertToString(eltVal);
				if (this.index.getAnnotation(Key.class).order() == 1) {
					assert this.incrementingField.getAnnotation(Key.class).order() == 2;
					eltRep = key + KeyManagement.aspectOf().getSeparator(this.clazz) + eltRep;
				} else {
					assert this.incrementingField.getAnnotation(Key.class).order() == 1;
					assert this.index.getAnnotation(Key.class).order() == 2;
					eltRep = eltRep + KeyManagement.aspectOf().getSeparator(this.clazz) + key;
				}
				this.collection.put(key, this.convert(key, ConversionTools.convert(eltRep)));
			} else {
				this.collection.put(key, this.convert(key, elements.get(key)));
			}
		}
		this.activated = true;
	}

	/**
	 * Returns the number of activated elements.
	 */
	@Override
	public int size() {
		return this.collection.size();
	}
	
//	Given up ; not efficient way to do that in HBase 0.20.6
//	/**
//	 * Returns the number of elements in the data store.
//	 */
//	public int sizeInStore() throws DatabaseNotReachedException {
//		return this.getOwner().getStore().count(this.ownerTable, this.getOwner().getIdentifier(), this.getName());
//	}

	/**
	 * Checks whether this column family is empty.
	 * If no element is cached, requests the data store.
	 */
	@Override
	public boolean isEmpty() {
		return this.collection.isEmpty();
	}

	/**
	 * Checks whether this column family is empty in the data store.
	 */
	public boolean isEmptyInStore() throws DatabaseNotReachedException {
		return this.getOwner().getStore().exists(this.ownerTable, this.getOwner().getIdentifier(), this.getName());
	}

	/**
	 * Check whether this element exists in the activated elements.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean contains(final Object o) {
		String id;
		try {
			id = this.getIndex((T)o);
		} catch (ClassCastException x) {
			return false;
		}
		
		if (this.collection.containsKey(id))
			return true;
		
		if (this.changes.containsKey(id)) {
			assert this.changes.get(id).equals(ChangeKind.DELETE);
			return false;
		}
		
		return false;
	}


	/**
	 * Check whether this element exists in the family.
	 * If this element is unknown in the cache, it triggers a request to the data store.
	 * In the case it exists, the element is added as an activated element to the collection.
	 */
	@SuppressWarnings("unchecked")
	public boolean containsInStore(final Object o) throws DatabaseNotReachedException {
		String id;
		try {
			id = this.getIndex((T)o);
		} catch (ClassCastException x) {
			return false;
		}
		
		return this.getFromStore(id) != null;
	}

	/**
	 * Supplies an iterator over activated values only.
	 */
	@Override
	public Iterator<T> iterator() {
		return this.collection.values().iterator();
	}

	/**
	 * Supplies an array that contains activated values only.
	 */
	@Override
	public Object[] toArray() {
		return this.collection.values().toArray();
	}

	/**
	 * Supplies an array that contains activated values only.
	 */
	@SuppressWarnings("hiding")
	@Override
	public <T> T[] toArray(T[] a) {
		return this.collection.values().toArray(a);
	}

	/**
	 * Adds an element to the column family.
	 * For this element to appear in the datastore, the owner object must be called the {@link #PersistingElement.store()} method
	 */
	@Override
	public boolean add(T o) {
		if (o == null)
			return false;
		String index = this.getIndex(o);
		T old = this.collection.put(index, o);
		if (this.incrementingField != null) {
			Number oVal = (Number) (old == null ? null : PropertyManagement.aspectOf().candideReadValue(old, incrementingField));
			Number nVal = (Number) PropertyManagement.aspectOf().candideReadValue(o, incrementingField);
			try {
				this.increments.put(index, IncrementManagement.aspectOf().getActualIncrement(nVal, oVal, this.getIncrement(index), incrementingField));
			} catch (Exception x) {
				return false;
			}
		} else {
			if (old == null || !old.equals(o))
				this.changes.put(index, ChangeKind.SET);
		}
		return true;
	}

	/**
	 * Removes an element to the column family.
	 * For this element not to appear anymore in the datastore, the owner object must be called the {@link #PersistingElement.store()} method.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean remove(Object o) {
		String index = this.getIndex((T) o);
		try {
			this.removeKey(index);
			return true;
		} catch (RuntimeException x) {
			return false;
		}
	}

	/**
	 * Returns true if this collection contains all of the elements in the collection of activated values only.
	 */
	@Override
	public boolean containsAll(Collection<?> c) {
		for (Object object : c) {
			if (!this.contains(object))
				return false;
		}
		return true;
	}


	/**
	 * Adds elements to the column family.
	 * For those elements to appear in the datastore, the owner object must be called the {@link #PersistingElement.store()} method
	 */
	@Override
	public boolean addAll(Collection<? extends T> c) {
		for (T t : c) {
			if (!this.add(t))
				return false;
		};
		return true;
	}


	/**
	 * Removes elements to the column family.
	 * For those elements to appear in the datastore, the owner object must be called the {@link #PersistingElement.store()} method.
	 */
	@Override
	public boolean removeAll(Collection<?> c) {
		for (Object t : c) {
			this.remove(t);
		};
		return true;
	}

	/**
	 * Retains only the elements in this collection that are contained in the specified collection of activated elements.
	 * In other words, removes from this collection all of its elements that are not contained in the specified collection.
	 * For those elements not to appear anymore in the datastore, the owner object must be called the {@link #PersistingElement.store()} method.
	 */
	@Override
	public boolean retainAll(Collection<?> c) {
		for (Object object : c) {
			if (this.contains(object))
				this.remove(object);
		}
		return true;
	}


	/**
	 * Removes all activated elements from the column family.
	 * For those activated elements not to appear anymore in the datastore, the owner object must be called the {@link #PersistingElement.store()} method.
	 */
	@Override
	public void clear() {
		this.removeAll(this);
	}
	
	/**
	 * The set of identifiers for activated elements.
	 */
	public Set<String> keySet() {
		return this.collection.keySet();
	}

	/**
	 * Removes an element to the column family given its key.
	 * For this element not to appear anymore in the datastore, the owner object must be called the {@link #PersistingElement.store()} method.
	 */
	public void removeKey(String key) {
		if (this.isAddOnly())
			throw new IllegalStateException("This collection does not accepts removal.");
		if (this.collection.containsKey(key))
			this.collection.remove(key);
		assert this.changes != null && this.increments == null;
		this.changes.put(key, ChangeKind.DELETE);
	}


	/**
	 * Finds an cached element according to its key.
	 */
	public T get(String key) {
		if (this.collection.containsKey(key))
			return this.collection.get(key);
		if (this.changes.containsKey(key)) {
			assert this.changes.get(key).equals(ChangeKind.DELETE);
			return null;
		}
		assert this.increments == null || !this.increments.containsKey(key);
		return null;
	}


	/**
	 * Finds an element according to its key.
	 * If the element is not in the cache, attempts to get it from the data store.
	 * The found element goes into the cache.
	 */
	public T getFromStore(String key) throws DatabaseNotReachedException {
		//First, tries from the cache
		if (this.collection.containsKey(key))
			return this.collection.get(key);
		if (this.changes != null && this.changes.containsKey(key)) {
			assert this.changes.get(key).equals(ChangeKind.DELETE);
			return null;
		}
		assert this.increments == null || !this.increments.containsKey(key);
		
		byte[] res = this.owner.getStore().get(this.ownerTable, this.owner.getIdentifier(), this.name, key);
		if (res == null)
			return null;
		T element = this.convert(key, res);
		if (this.changes != null)
			this.changes.remove(key);
		if (!this.getIndex(element).equals(key))
			throw new Error("Found element with key " + key + " with a different key " + this.getIndex(element) + " (row '" + this.ownerTable +"'/'" + this.owner.getIdentifier() + "'/'"+ this.name + ")");
		this.collection.put(key, element);
		return element;
	}
	
	protected T convert(String key, byte [] rep) {
		return ConversionTools.convert(this.clazz, rep);
	}
	
	public Set<String> changedKeySet() {
		return this.changes == null ? new TreeSet<String>() : this.changes.keySet();
	}
	
	public Set<String> incrementedKeySet() {
		return this.increments == null ? new TreeSet<String>() : this.increments.keySet();
	}
	
	public boolean hasChanged() {
		return (this.changes != null && !this.changes.isEmpty())
				|| (this.increments != null && !this.increments.isEmpty());
	}
	
	public boolean wasChanged(String key) {
		return (this.changes != null && this.changes.containsKey(key)&& this.changes.get(key).equals(ChangeKind.SET))
			|| (this.increments != null && this.increments.containsKey(key));
	}
	
	public boolean wasDeleted(String key) {
		return this.changes != null && this.changes.containsKey(key)&& this.changes.get(key).equals(ChangeKind.DELETE);
	}
	
	public Number getIncrement(String key) {
		return this.increments == null ? null : this.increments.containsKey(key) ? this.increments.get(key) : null;
	}
	
	void clearChanges() {
		if (this.changes != null)
			this.changes.clear();
		if (this.increments != null)
			this.increments.clear();
	}

	@Override
	public int hashCode() {
		return this.name.hashCode();
	}
}
