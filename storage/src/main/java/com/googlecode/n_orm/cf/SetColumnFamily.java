package com.googlecode.n_orm.cf;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.IncrementException;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.conversion.ConversionTools;

/**
 * Column family for sets.
 * Elements are stored using their identifier as key (see {@link PersistingElement#getIdentifier()} and {@link PersistingElement#getFullIdentifier()}), and an empty byte array as value.
 * Elements stored in this set should not be mutable, as they are referred to using their key, that is if it changes afterwards, it's its key value that is actually used to be stored.
 * This Set does not support null values.
 * Elements are not actually stored in a real Set, its only their key that is stored by this set. However, retrieving an element from this set is still efficient thanks to the cache (see {@link KeyManagement#createElement(Class, String)}.
 */
public class SetColumnFamily<T> extends ColumnFamily<byte[]> implements Set<T> {
	/**
	 * The actual value used in the data store;
	 */
	private static final byte [] value = new byte[0];
	
	private class SetColumnFamilyIterator implements Iterator<T> {
		
		private final Iterator<String> elements;
		private String current;
		
		public SetColumnFamilyIterator(Iterator<String> elements) {
			this.elements = elements;
		}

		@Override
		public boolean hasNext() {
			return elements.hasNext();
		}

		@Override
		public T next() {
			this.current = elements.next();
			return KeyManagement.getInstance().createElement(getSetElementClazz(), this.current);
		}

		@Override
		public void remove() {
			if (current == null)
				throw new IllegalStateException("Should beiterating over an element ; either removing before calling next, or removing twice.");
			SetColumnFamily.this.removeKey(current);
			current = null;
		}
		
	}
	
	/**
	 * SetColumnFamily inherits ColumnFamily<byte[]> so {@link #getClazz()} returns byte[].class ; this method returns the actual type of expected elements.
	 */
	private final Class<T> setElementClazz;
	
	public SetColumnFamily() {
		setElementClazz = null;
	}

	public SetColumnFamily(Class<T> clazz, Field property,
			PersistingElement owner) throws SecurityException, NoSuchFieldException {
		this(clazz, property, property.getName(), owner);
	}

	public SetColumnFamily(Class<T> clazz, Field property, String name,
			PersistingElement owner) {
		super(byte[].class, property, name, owner);
		this.setElementClazz = clazz;
	}
	
	public Class<T> getSetElementClazz() {
		return setElementClazz;
	}

	@Override
	public Serializable getSerializableVersion() {
		return new HashSet<T>(this);
	}

	protected String getIndex(T object) {
		return ConversionTools.convertToString(object, this.getSetElementClazz());
	}

	@Override
	protected byte[] preparePut(String key, byte[] rep) {
		assert rep.length == 0;
		return rep;
	}

	/**
	 * Adds an element to the column family.
	 * For this element to appear in the datastore, the owner object must be called the {@link PersistingElement#store()} method
	 */
	@Override
	public boolean add(T o) {
		if (o == null)
			throw new NullPointerException("Set collection family " + this.getName() + " for persisting " + this.getOwner() + " does not support null values.");
		String index;
		try {
			index = this.getIndex(o);
		} catch (IllegalArgumentException x) {
			throw x;
		} catch (Exception x) {
			throw new IllegalArgumentException("Cannot extract representation for " + o + ": " + x.getMessage(), x);
		}
		try {
			if (this.containsKey(index))
				return false;
			putElement(index, value);
			return true;
		} catch (IncrementException e) {
			assert false;
			throw new IllegalStateException(e);
		} finally {
			this.contains(o);
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
		} catch (IllegalArgumentException x) {
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
	 * For this element not to appear anymore in the datastore, the owner object must be called the {@link PersistingElement#store()} method.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean remove(Object o) {
		String index = this.getIndex((T) o);
		try {
			if (!this.containsKey(index))
				return false;
			this.removeKey(index);
			return true;
		} catch (RuntimeException x) {
			return false;
		} finally {
			assert ! this.contains(o);
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
	 * For those elements to appear in the datastore, the owner object must be called the {@link PersistingElement#store()} method
	 */
	@Override
	public boolean addAll(Collection<? extends T> c) {
		boolean ret = false;
		for (T t : c) {
			if (this.add(t))
				ret = true;
		};
		return ret;
	}


	/**
	 * Removes elements to the column family.
	 * For those elements to appear in the datastore, the owner object must be called the {@link PersistingElement#store()} method.
	 */
	@Override
	public boolean removeAll(Collection<?> c) {
		boolean ret = false;
		for (Object t : c) {
			if (this.remove(t))
				ret = true;
		};
		return ret;
	}

	/**
	 * Retains only the elements in this collection that are contained in the specified collection of activated elements.
	 * In other words, removes from this collection all of its elements that are not contained in the specified collection.
	 * For those elements not to appear anymore in the datastore, the owner object must be called the {@link PersistingElement#store()} method.
	 */
	@Override
	public boolean retainAll(Collection<?> c) {
		boolean ret = false;
		Iterator<T> it = this.iterator();
		while (it.hasNext()) {
			if (! c.contains(it.next())) {
				it.remove();
				ret = true;
			}
		}
		return ret;
	}


	/**
	 * Removes all activated elements from the column family.
	 * For those activated elements not to appear anymore in the datastore, the owner object must be called the {@link PersistingElement#store()} method.
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
		return new SetColumnFamilyIterator(new TreeSet<String>(this.collection.keySet()).iterator());
	}

	/**
	 * Supplies an array that contains activated values only.
	 */
	@Override
	public Object[] toArray() {
		return this.collection.keySet().toArray();
	}

	/**
	 * Supplies an array that contains activated values only.
	 */
	@SuppressWarnings("hiding")
	@Override
	public <T> T[] toArray(T[] a) {
		return this.collection.keySet().toArray(a);
	}

	@Override
	public boolean equals(Object obj) {
		return obj != null && obj instanceof Set && this.containsAll((Set)obj) && ((Set)obj).containsAll(this);
	}

	@Override
	public int hashCode() {
		return this.getKeys().hashCode();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void activate(Object from, Object to)
			throws DatabaseNotReachedException {
		if (from != null && ! this.getSetElementClazz().isInstance(from))
			throw new IllegalArgumentException(from.toString() + " is not compatible with " + this.getSetElementClazz());
		if (to != null && ! this.getSetElementClazz().isInstance(to))
			throw new IllegalArgumentException(to.toString() + " is not compatible with " + this.getSetElementClazz());
		super.activate(this.getIndex((T) from), this.getIndex((T) to));
	}


	@Override
	protected void updateFromPOJO(Object pojo) {
		Set<String> keys = new TreeSet<String>(this.getKeys());
		
		@SuppressWarnings("unchecked")
		Set<T> pojoS = (Set<T>)pojo;
		for (T element : pojoS) {
			String key = this.getIndex(element);
			if (!keys.remove(key)) {
				this.add(element);
			}
			if (this.slow) { //For test purpose
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
				}
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
	protected void addToPOJO(Object pojo, String key, byte[] element) {
		@SuppressWarnings("unchecked")
		Set<T> pojoS = (Set<T>)pojo;
		T actualElement = KeyManagement.getInstance().createElement(this.getSetElementClazz(), key);
		pojoS.add(actualElement);
	}
	
	private boolean slow = false;
	
	/**
	 * For test purpose...
	 */
	void goSlow() {
		this.slow = true;
	}
}
