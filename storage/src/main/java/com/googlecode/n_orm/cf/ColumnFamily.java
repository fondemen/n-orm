package com.googlecode.n_orm.cf;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.googlecode.n_orm.AddOnly;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.DecrementException;
import com.googlecode.n_orm.IncrementManagement;
import com.googlecode.n_orm.Incrementing;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.storeapi.Constraint;


public abstract class ColumnFamily<T> {
	public static enum ChangeKind {SET, DELETE};
	
	protected final Class<T> clazz;
	protected final Field property;
	protected final String name;
	protected final PersistingElement owner;
	protected final String ownerTable;

	protected final Map<String, T> collection = new TreeMap<String, T>();

	protected Map<String, ChangeKind> changes;
	protected final Map<String, Number> increments;
	protected final boolean addOnly;
	
	protected boolean activated = false;
	
	public ColumnFamily() { 
		this.clazz = null;
		this.property = null;
		this.name = null;
		this.owner = null;
		this.ownerTable = null;
		this.activated = false;
		this.increments = null;
		this.addOnly = false;
	}

	public ColumnFamily(Class<T> clazz, Field property, String name, PersistingElement owner) {
		super();
		this.clazz = clazz;
		this.property = property;
		this.name = name;
		this.owner = owner;
		this.ownerTable = this.owner.getTable();
		this.addOnly = property != null && property.isAnnotationPresent(AddOnly.class);
		if (property != null && property.isAnnotationPresent(Incrementing.class)) {
			if (!Number.class.isAssignableFrom(clazz))
				throw new IllegalArgumentException("Only number types may be incrementing, which is not the case of elements of " + property);
			this.increments = new TreeMap<String, Number>();
			this.changes = null;
		} else {
			this.changes = new TreeMap<String, ChangeKind>();
			this.increments = null;
		}
	}
	
	public abstract Serializable getSerializableVersion();
	protected abstract void updateFromPOJO(Object pojoVersion);
	protected abstract void storeToPOJO(Object pojoVersion);
	protected abstract void addToPOJO(Object pojoVersion, String key, T element);
	
	public String getName() {
		return name;
	}
	
	public Class<T> getClazz() {
		return clazz;
	}

	/**
	 * @return the property corresponding to that column family ; may be null, e.g. for the properties or increments column families
	 */
	public Field getProperty() {
		return property;
	}

	public PersistingElement getOwner() {
		return owner;
	}

	public boolean isAddOnly() {
		return this.addOnly || this.increments != null;
	}

	public boolean isActivated() {
		return activated;
	}
	
	public void assertIsActivated(String messageToDescribeTheContextOfTheCheck) throws IllegalStateException {
		if (! this.isActivated())
			throw new IllegalStateException("Column family " + this.getName() + " should be activated on " + this.getOwner() + " while " + messageToDescribeTheContextOfTheCheck);
	}
	
	public void activate() throws DatabaseNotReachedException {
		this.activate(null);
	}
	
	public abstract void activate(Object from, Object to) throws DatabaseNotReachedException;
	
	public void activate(String fromIndex, String toIndex) throws DatabaseNotReachedException {
		this.activate(new Constraint(fromIndex, toIndex));
	}
	
	public void activate(Constraint c) throws DatabaseNotReachedException {
		this.owner.checkIsValid();
		String id = this.owner.getIdentifier();
		assert id != null;
		Map<String, byte[]> elements = c == null ? this.owner.getStore().get(this.ownerTable, id, this.name) : this.owner.getStore().get(this.ownerTable, id, this.name, c);
		this.rebuild(elements);
	}

	public void rebuild(Map<String, byte[]> rawData) throws DatabaseNotReachedException {
		this.collection.clear();
		this.clearChanges();
		String id = this.owner.getIdentifier();
		assert id != null;
		for (Entry<String, byte[]> entry : rawData.entrySet()) {
			this.collection.put(entry.getKey(), this.preparePut(entry.getKey(), entry.getValue()));
		}
		markActivated();
		this.storeToPOJO();
		assert ! this.hasChanged();
	}

	private void markActivated() {
		this.activated = true;
	}
	
	protected T preparePut(String key, byte [] rep) {
		return ConversionTools.convert(this.clazz, rep);
	}

	/**
	 * Returns the number of activated elements.
	 */
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
	public boolean isEmpty() {
		return this.collection.isEmpty();
	}

	/**
	 * Checks whether this column family is empty in the data store.
	 */
	public boolean isEmptyInStore() throws DatabaseNotReachedException {
		return !this.getOwner().getStore().exists(this.ownerTable, this.getOwner().getIdentifier(), this.getName());
	}

	public boolean containsKey(String key) {
		if (this.collection.containsKey(key))
			return true;
		
		if (this.changes != null && this.changes.containsKey(key)) {
			assert this.changes.get(key).equals(ChangeKind.DELETE);
			return false;
		}
		
		assert this.increments == null || !this.increments.containsKey(key);
		
		return false;
	}

	public void putElement(String key, T element) throws DecrementException {
		T old = this.collection.put(key, element);
		if (this.increments != null) {
			Number oVal = (Number) old;
			Number nVal = (Number) element;
			this.increments.put(key, IncrementManagement.getInstance().getActualIncrement(nVal, oVal, this.getIncrement(key), this.getProperty()));
		} else {
			if (element == null)
				this.removeKey(key);
			else if (old == null || this.hasChanged(key, old, element))
				this.changes.put(key, ChangeKind.SET);
		}
	}
	
	protected boolean hasChanged(String key, T lhs, T rhs) {
		if(lhs == rhs)
			return false;
		
		return !Arrays.equals(ConversionTools.convert(lhs, this.clazz), ConversionTools.convert(rhs, this.clazz));
	}
	
	/**
	 * The set of identifiers for activated elements.
	 */
	public Set<String> getKeys() {
		return this.collection.keySet();
	}

	/**
	 * Removes an element to the column family given its key.
	 * For this element not to appear anymore in the datastore, the owner object must be called the {@link PersistingElement#store()} method.
	 */
	public void removeKey(String key) {
		if (this.isAddOnly())
			throw new IllegalStateException("This collection does not accepts removal.");
		if (this.collection.containsKey(key)) {
			this.collection.remove(key);
			assert this.changes != null && this.increments == null;
			this.changes.put(key, ChangeKind.DELETE);
		}
	}


	/**
	 * Finds an cached element according to its key.
	 */
	public T getElement(String key) {
		T ret = this.collection.get(key);
		if (ret != null)
			return ret;
		if (this.changes != null && this.changes.containsKey(key)) {
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
		T element = this.preparePut(key, res);
		if (this.changes != null)
			this.changes.remove(key);
		this.collection.put(key, element);
		this.addToPOJO(this.getPOJO(true), key, element);
		return element;
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
	
	public void clearChanges() {
		if (this.changes != null)
			this.changes.clear();
		if (this.increments != null)
			this.increments.clear();
	}

	@Override
	public int hashCode() {
		return this.name.hashCode();
	}
	
	public Object getPOJO(boolean createIfNull) {
		Object pojo = PropertyManagement.getInstance().candideReadValue(this.getOwner(), this.getProperty());
		if (pojo == null && createIfNull) {
			pojo = this.getSerializableVersion();
			PropertyManagement.getInstance().candideSetValue(this.getOwner(), this.getProperty(), pojo);
		}
		
		return pojo;
	}
	
	public void updateFromPOJO() {
		if (this.getProperty() == null) {
			assert false : "Shouldn't update column family " + this.getName();
			return;
		}

		Object pojo = this.getPOJO(false);
		if (pojo != null && pojo != this) {
			this.updateFromPOJO(pojo);
		}
	}
	
	public void storeToPOJO() {
		if (this.getProperty() == null) {
			assert false : "Shouldn't store column family " + this.getName();
			return;
		}

		Object pojo = this.getPOJO(true);
		if (pojo != null && pojo != this) {
			this.storeToPOJO(pojo);
		}
	}
}
