package com.googlecode.n_orm.cf;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import com.googlecode.n_orm.AddOnly;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.DecrementException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PropertyManagement;


public class SetColumnFamily<T> extends ColumnFamily<T> implements Set<T> {
	private final Field index;
	
	public SetColumnFamily() {
		index = null;
	}

	public SetColumnFamily(Class<T> clazz, Field property,
			PersistingElement owner, String index) throws SecurityException, NoSuchFieldException {
		this(clazz, property, property.getName(), owner, PropertyManagement.getInstance().getProperty(clazz, index));
	}

	public SetColumnFamily(Class<T> clazz, Field property, String name,
			PersistingElement owner, Field index) {
		super(clazz, property, name, owner);
		this.index = index;
	}
	
	@Override
	public Serializable getSerializableVersion() {
		return new HashSet<T>(this.collection.values());
	}

	protected String getIndex(T object) {
		return PropertyManagement.getInstance().candideReadValue(object, this.index).toString();
	}
	
	@Override
	public T getFromStore(String key) throws DatabaseNotReachedException {
		T ret = super.getFromStore(key);

		if (ret != null && !this.getIndex(ret).equals(key))
			throw new Error("Found element with key " + key + " with a different key " + this.getIndex(ret) + " (row '" + this.ownerTable +"'/'" + this.owner.getIdentifier() + "'/'"+ this.name + ")");
		
		return ret;
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
		try {
			putElement(index, o);
			return true;
		} catch (DecrementException e) {
			assert false;
			throw new IllegalStateException(e);
		}
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
		
		return containsKey(id);
	}
	
	/**
	 * Checks whether this element exists in the family.
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

	@SuppressWarnings("unchecked")
	@Override
	public void activate(Object from, Object to)
			throws DatabaseNotReachedException {
		if (! this.clazz.isInstance(from))
			throw new IllegalArgumentException(from.toString() + " is not compatible with " + this.clazz);
		if (! this.clazz.isInstance(to))
			throw new IllegalArgumentException(to.toString() + " is not compatible with " + this.clazz);
		super.activate(this.getIndex((T) from), this.getIndex((T) to));
	}


	@Override
	protected void updateFromPOJO(Object pojo) {
		Set<String> keys = new TreeSet<String>(this.getKeys());
		
		@SuppressWarnings("unchecked")
		Set<T> pojoS = (Set<T>)pojo;
		for (T element : new ArrayList<T>(pojoS)) {
			String key = this.getIndex(element);
			if (keys.remove(key)) {
				T known = this.getElement(key);
				if (known == null || this.hasChanged(key, known, element))
					this.add(element);
			} else {
				this.add(element);
			}
		}
		
		for (String key : keys) {
			this.removeKey(key);
		}
	}

	@Override
	protected void storeToPOJO(Object pojo) {
		@SuppressWarnings("unchecked")
		Set<T> pojoS = (Set<T>)pojo;
		pojoS.addAll(this);
	}

	@Override
	protected void addToPOJO(Object pojo, String key, T element) {
		@SuppressWarnings("unchecked")
		Set<T> pojoS = (Set<T>)pojo;
		
		pojoS.add(element);
	}
}
