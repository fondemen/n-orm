package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.WeakHashMap;


import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.ExplicitActivation;
import com.googlecode.n_orm.ImplicitActivation;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PersistingMixin;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Row;
import com.googlecode.n_orm.storeapi.Store;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.query.ConstraintBuilder;
import com.googlecode.n_orm.storeapi.Constraint;

public aspect StorageManagement {
	//Dangerous: a subclass would need to store one more column family (i.e. alter the data store metadata) which may be long even if this information is never read
//	public static final String CLASS_COLUMN_FAMILY = "class";
//	public static final String CLASS_COLUMN = "";
	
	transient Boolean PersistingElement.exists = null;
	private transient boolean PersistingElement.isStoring = false;
	private transient Collection<Class<? extends PersistingElement>> PersistingElement.persistingSuperClasses = null;
	
	public boolean PersistingElement.isKnownAsExistingInStore() {
		return this.exists == Boolean.TRUE;
	}
	
	public boolean PersistingElement.isKnownAsNotExistingInStore() {
		return this.exists == Boolean.FALSE;
	}
	
	public void PersistingElement.delete() throws DatabaseNotReachedException {
		this.getStore().delete(this.getTable(), this.getIdentifier());
		Collection<Class<? extends PersistingElement>> psc = this.getPersistingSuperClasses();
		if (!psc.isEmpty()) {
			PersistingMixin px = PersistingMixin.getInstance();
			for (Class<? extends PersistingElement> cls : psc) {
				this.getStore().delete(px.getTable(cls), this.getFullIdentifier());
			}
		}
		this.exists= Boolean.FALSE;
	}
	
	public void PersistingElement.store() throws DatabaseNotReachedException {
		this.checkIsValid();
		
		synchronized(this) {
			if (this.isStoring)
				return;
			isStoring = true;
		}
		try {
			this.updateFromPOJO();
			
			Persisting annotation = this.getClass().getAnnotation(Persisting.class);
			
			PropertyManagement pm = PropertyManagement.getInstance();
			Map<String, Map<String, byte[]>> changed = new TreeMap<String, Map<String,byte[]>>(), localChanges;
			Map<String, Set<String>> deleted = new TreeMap<String, Set<String>>();
			Map<String, Map<String, Number>> increments = new TreeMap<String, Map<String,Number>>();
			Map<String,Number> propsIncrs = this.getIncrements();
			if (!propsIncrs.isEmpty())
				increments.put(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, propsIncrs);
			Set<ColumnFamily<?>> families = this.getColumnFamilies();
			for (ColumnFamily<?> family : families) {
				Set<String> changedKeys = family.changedKeySet();
				if (!changedKeys.isEmpty()) {
					Map<String, byte[]> familyChanges = new TreeMap<String, byte[]>();
					Set<String> familyDeleted = new TreeSet<String>();
					Field cfField = family.getProperty();
					for (String key : changedKeys) {
						if (family.wasDeleted(key))
							familyDeleted.add(key);
						else {
							//No need for auto-loading for it is a changed value
							Object element = family.getElement(key);
							Class<?> expected;
							if (cfField != null) {
								expected = family.getClazz();
							} else if (element instanceof PropertyManagement.Property) {
								Field propField = ((PropertyManagement.Property)element).getField();
								assert propField != null;
								expected = propField.getType();
							} else {
								assert false;
								expected = element.getClass();
							}
							familyChanges.put(key, ConversionTools.convert(element, expected));
						}
					}
					if (!familyChanges.isEmpty())
						changed.put(family.getName(), familyChanges);
					if (!familyDeleted.isEmpty())
						deleted.put(family.getName(), familyDeleted);
				}
				Set<String> incrementedKeys = family.incrementedKeySet();
				if (!incrementedKeys.isEmpty()) {
					Map<String, Number> familyIncr = new TreeMap<String,Number>();
					increments.put(family.getName(), familyIncr);
					for (String key : incrementedKeys) {
						familyIncr.put(key, family.getIncrement(key));
					}
				}
			}
			
			//Storing keys into properties. As keys are final, there is no need to store them again if we know that the object already exists within the base
			if (annotation.storeKeys() && (this.exists == null || this.exists.equals(Boolean.FALSE))) {
				localChanges = new TreeMap<String, Map<String,byte[]>>(changed);
				Map<String, byte[]> changedProperties = changed.get(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME);
				if (changedProperties == null) {
					changedProperties = new TreeMap<String, byte[]>();
				} else {
					changedProperties = new TreeMap<String, byte[]>(changedProperties);
				}
				localChanges.put(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, changedProperties);
				for (Field key : this.getKeys()) {
					try {
						changedProperties.put(key.getName(), ConversionTools.convert(pm.readValue(this, key), key.getType()));
					} catch (RuntimeException e) {
						throw e;
					} catch (Exception e) {
						throw new IllegalStateException("Cannot save object ; problem reading property : " + e.getMessage(), e);
					}
				}
			} else
				localChanges = changed;
			
			if (!(this.exists == Boolean.TRUE && changed.isEmpty() && deleted.isEmpty() && increments.isEmpty())) {
				
				this.getStore().storeChanges(this.getTable(), this.getIdentifier(), localChanges, deleted, increments);
	
				propsIncrs.clear();
				for(ColumnFamily<?> family : families) {
					family.clearChanges();
				}
				
				//Storing in persisting superclasses
				Collection<Class<? extends PersistingElement>> persistingSuperClasses = this.getPersistingSuperClasses();
				if (!persistingSuperClasses.isEmpty()) {
					PersistingMixin px = PersistingMixin.getInstance();
					//The next line to avoid repeating all properties in superclasses
					if (!annotation.storeAlsoInSuperClasses()) {
						changed.clear(); deleted.clear(); increments.clear();
					}
//					Map<String, byte[]> classColumn = new TreeMap<String, byte[]>();
//					String clsName = this.getClass().getName();
//					classColumn.put(CLASS_COLUMN, ConversionTools.convert(clsName, String.class));
//					changed.put(CLASS_COLUMN_FAMILY, classColumn);
					String ident = this.getFullIdentifier();
					for (Class<? extends PersistingElement> sc : persistingSuperClasses) {
						this.getStore().storeChanges(px.getTable(sc), ident, changed, deleted, increments);
					}
				}
			}
			
			//Store depending properties
			for (Field prop : pm.getProperties(this.getClass())) {
				if (pm.isPersistingPropertyType(prop.getType())) {
					Object kVal = pm.candideReadValue(this, prop);
					if (kVal != null)
						((PersistingElement)kVal).store();
				}
			}
			
			this.exists= Boolean.TRUE;
		} finally {
			synchronized(this) {
				isStoring = false;
			}
		}
	}

	@SuppressWarnings("unchecked")
	public Collection<Class<? extends PersistingElement>> PersistingElement.getPersistingSuperClasses() {
		if (this.persistingSuperClasses != null)
			return this.persistingSuperClasses;
		
		this.persistingSuperClasses = new LinkedList<Class<? extends PersistingElement>>();
		Class<?> sp = this.getClass().getSuperclass();
		Class<? extends PersistingElement> spPers;
		while (sp != null) {
			if (sp.isAnnotationPresent(Persisting.class)) {
				spPers = (Class<? extends PersistingElement>) sp;
				this.persistingSuperClasses.add(spPers);
			}
			sp = sp.getSuperclass();
		}
		return this.persistingSuperClasses;
	}
	
	public void PersistingElement.activateColumnFamily(String name) throws DatabaseNotReachedException {
		this.getColumnFamily(name).activate();
	}
	
	public void PersistingElement.activateColumnFamily(String name, Object fromObject, Object toObject) throws DatabaseNotReachedException {
		this.getColumnFamily(name).activate(fromObject, toObject);
	}
	
	public void PersistingElement.activateColumnFamilyIfNotAlready(String name) throws DatabaseNotReachedException {
		ColumnFamily<?> cf = this.getColumnFamily(name);
		if (!cf.isActivated())
			cf.activate();
	}
	
	public void PersistingElement.activateColumnFamilyIfNotAlready(String name, Object fromObject, Object toObject) throws DatabaseNotReachedException {
		ColumnFamily<?> cf = this.getColumnFamily(name);
		if (!cf.isActivated())
			cf.activate(fromObject, toObject);
	}
	
	public void PersistingElement.activateIfNotAlready(String... families) throws DatabaseNotReachedException {
		this.activate(false, families);
	}
	
	public void PersistingElement.activate(String... families) throws DatabaseNotReachedException {
		this.activate(true, families);
	}

	private void PersistingElement.activate(boolean force, String... families) throws DatabaseNotReachedException {
		this.checkIsValid();
		
		Set<String> toBeActivated = getActualFamiliesToBeActivated(force, families);
		
		if (! toBeActivated.isEmpty()) {
			Map<String, Map<String, byte[]>> rawData = this.getStore().get(this.getTable(), this.getIdentifier(), toBeActivated);
			activateFromRawData(toBeActivated, rawData);
		}
	}

	private void PersistingElement.activateFromRawData(Set<String> toBeActivated,
			Map<String, Map<String, byte[]>> rawData) {
		toBeActivated = new TreeSet<String>(toBeActivated);//Avoiding changing the initial collection
		
		for (Entry<String, Map<String, byte[]>> families : rawData.entrySet()) {
			this.getColumnFamily(families.getKey()).rebuild(families.getValue());
			boolean removed = toBeActivated.remove(families.getKey());
			assert removed : "Got unexpected column family " + families.getKey() + " from raw data for " + this;
		}
		
		if (!toBeActivated.isEmpty()) {
			Map<String, byte[]> emptyTree = new TreeMap<String, byte[]>();
			for (String tba : toBeActivated) {
				this.getColumnFamily(tba).rebuild(emptyTree);
			}
		}
	}

	private Set<String> PersistingElement.getActualFamiliesToBeActivated(boolean force, String... families) {
		Set<String> toBeActivated = StorageManagement.getAutoActivatedFamilies(this.getClass(), families);

		if (!force) {
			for (String family : new TreeSet<String>(toBeActivated)) {
				ColumnFamily<?> cf = this.getColumnFamily(family);
				if (cf.isActivated())
					toBeActivated.remove(family);
			}
		}
		return toBeActivated;
	}
	
	private static Set<String> getAutoActivatedFamilies(Class<? extends PersistingElement> clazz, String... families) {
		ColumnFamiliyManagement cfm = ColumnFamiliyManagement.getInstance();
		Set<String> toBeActivated = new TreeSet<String>();
		
		if (families == null)
			return toBeActivated;
		
		toBeActivated.add(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME);
		
		Set<String> cfs = new TreeSet<String>();
		for (Field cff : cfm.detectColumnFamilies(clazz)) {
			if (cff.getAnnotation(ImplicitActivation.class) != null)
				toBeActivated.add(cff.getName());
			cfs.add(cff.getName());
		}
		
		if (families != null) {
			for (String family : families) {
				if (! cfs.contains(family))
					throw new IllegalArgumentException("Unknown column family " + family + " in class " + clazz);
				toBeActivated.add(family);
			}
		}
		return toBeActivated;
	}
	
	public boolean PersistingElement.existsInStore() throws DatabaseNotReachedException {
		boolean ret = this.getStore().exists(this.getTable(), this.getIdentifier());
		this.exists = ret ? Boolean.TRUE : Boolean.FALSE;
		return ret;
	}
	
	public static <T> T getElement(Class<T> clazz, String identifier) {
		return KeyManagement.getInstance().createElement(clazz, identifier);
	}
	
	public static <T extends PersistingElement> CloseableIterator<T> findElement(final Class<T> clazz, Constraint c, final int limit, String... families) throws DatabaseNotReachedException {
		Store store = StoreSelector.getInstance().getStoreFor(clazz);
		final Set<String> toBeActivated = families == null ? null : getAutoActivatedFamilies(clazz, families);
		final CloseableKeyIterator keys = store.get(PersistingMixin.getInstance().getTable(clazz), c, limit, toBeActivated);
		try {
			CloseableIterator<T> ret = new CloseableIterator<T>() {
				private int returned = 0;
				private boolean closed = false;

				@Override
				public boolean hasNext() {
					if (closed)
						return false;
					boolean ret = returned < limit && keys.hasNext();
					if (! ret) 
						this.close();
					return ret;
				}

				@Override
				public T next() {
					if (!this.hasNext())
						throw new IllegalStateException("The list is empty");
					Row data = keys.next();
					try {
						T elt = ConversionTools.convertFromString(clazz, data.getKey());
						((PersistingElement)elt).exists = true;
						//assert (toBeActivated == null) == ((data.getValues() == null)  || (data.getValues().entrySet().isEmpty())); //may be false (e.g. no properties)
						if (toBeActivated != null) { //the element should be activated
							elt.activateFromRawData(toBeActivated, data.getValues());
						}
						return elt;
					} finally {
						returned++;
					}
				}

				@Override
				public void remove() {
					keys.remove();
				}

				@Override
				public void close() {
					if (closed)
						return;
					
					keys.close();
					this.closed = true;
				}
				
				@Override
				protected void finalize() throws Throwable {
					this.close();
					super.finalize();
				}
			};
			return ret;
		} catch (RuntimeException x) {
			if (keys != null)
				keys.close();
			throw x;
		}
	}
	
	public static <T extends PersistingElement> long countElements(Class<T> clazz, Constraint c) {
		Store store = StoreSelector.getInstance().getStoreFor(clazz);
		return store.count(PersistingMixin.getInstance().getTable(clazz), c);
	}
	
//	/**
//	 * WARNING: this function empties the cache for all elements of class clazz.
//	 */
//	public static <T extends PersistingElement> void truncateElements(Class<T> clazz, Constraint c) {
//		Store store = StoreSelector.getInstance().getStoreFor(clazz);
//		KeyManagement.getInstance().cleanupKnownPersistingElements();
//		store.truncate(PersistingMixin.getInstance().getTable(clazz), c);
//	}

	public static <T extends PersistingElement> NavigableSet<T> findElementsToSet(final Class<T> clazz, Constraint c, final int limit, String... families) throws DatabaseNotReachedException {
		CloseableIterator<T> found = findElement(clazz, c, limit, families);
		try {
			NavigableSet<T> ret = new TreeSet<T>();
			while (found.hasNext()) {
				ret.add(found.next());
			}
			//assert ret.size() == Math.min(limit, countElements(clazz, c));
			return ret;
		} finally {
			found.close();
		}
	}

	public static ConstraintBuilder findElements() {
		return new ConstraintBuilder();
	}
}
