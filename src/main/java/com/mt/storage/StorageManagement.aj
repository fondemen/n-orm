package com.mt.storage;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.mt.storage.cf.ColumnFamily;
import com.mt.storage.conversion.ConversionTools;
import com.mt.storage.query.ConstraintBuilder;

public aspect StorageManagement {
	public static final String CLASS_COLUMN_FAMILY = "class";
	public static final String CLASS_COLUMN = "";
	
	private transient Boolean PersistingElement.exists = null;
	private transient boolean PersistingElement.isStoring = false;
	private transient Collection<Class<? extends PersistingElement>> PersistingElement.persistingSuperClasses = null;
	
	public void PersistingElement.delete() throws DatabaseNotReachedException {
		this.getStore().delete(this.getTable(), this.getIdentifier());
		Collection<Class<? extends PersistingElement>> psc = this.getPersistingSuperClasses();
		if (!psc.isEmpty()) {
			PersistingMixin px = PersistingMixin.getInstance();
			for (Class<? extends PersistingElement> cls : psc) {
				this.getStore().delete(px.getTable(cls), this.getFullIdentifier());
			}
		}
	}
	
	public void PersistingElement.store() throws DatabaseNotReachedException {
		if (this.getIdentifier() == null)
			throw new IllegalArgumentException("Cannot activate " + this + " before all its keys are valued.");
		this.checkKeys();
		
		synchronized(this) {
			if (this.isStoring)
				return;
			isStoring = true;
		}
		try {
			PropertyManagement pm = PropertyManagement.getInstance();
			this.storeProperties();
			Map<String, Map<String, byte[]>> changed = new TreeMap<String, Map<String,byte[]>>();
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
			if (this.exists == null || this.exists.equals(Boolean.FALSE)) {
				Map<String, byte[]> changedProperties = changed.get(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME);
				if (changedProperties == null) {
					changedProperties = new TreeMap<String, byte[]>();
					changed.put(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, changedProperties);
				}
				for (Field key : this.getKeys()) {
					try {
						changedProperties.put(key.getName(), ConversionTools.convert(pm.readValue(this, key), key.getType()));
					} catch (RuntimeException e) {
						throw e;
					} catch (Exception e) {
						throw new IllegalStateException("Cannot save object ; problem reading property : " + e.getMessage(), e);
					}
				}
			}
			
			this.getStore().storeChanges(this.getTable(), this.getIdentifier(), changed, deleted, increments);

			propsIncrs.clear();
			for(ColumnFamily<?> family : families) {
				family.clearChanges();
			}
			
			//Store depending properties
			for (Field prop : pm.getProperties(this.getClass())) {
				if (pm.isPersistingPropertyType(prop.getType()) && !prop.isAnnotationPresent(ExplicitActivation.class)) {
					Object kVal = pm.candideReadValue(this, prop);
					if (kVal != null)
						((PersistingElement)kVal).store();
				}
			}
			
			//Storing in persisting superclasses
			Collection<Class<? extends PersistingElement>> persistingSuperClasses = this.getPersistingSuperClasses();
			if (!persistingSuperClasses.isEmpty()) {
				PersistingMixin px = PersistingMixin.getInstance();
				Map<String, byte[]> classColumn = new TreeMap<String, byte[]>();
				String clsName = this.getClass().getName();
				classColumn.put(CLASS_COLUMN, ConversionTools.convert(clsName, String.class));
				//The next line to avoid repeating all properties in superclasses
				changed.clear(); deleted.clear(); increments.clear();
				changed.put(CLASS_COLUMN_FAMILY, classColumn);
				String ident = this.getFullIdentifier();
				for (Class<? extends PersistingElement> sc : persistingSuperClasses) {
					this.getStore().storeChanges(px.getTable(sc), ident, changed, deleted, increments);
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

	public void PersistingElement.activate(String... families) throws DatabaseNotReachedException {
		this.checkKeys();
		if (this.getIdentifier() == null)
			throw new IllegalArgumentException("Cannot activate " + this + " before all its keys are valued.");
		
		Set<String> toBeActivated = new TreeSet<String>();
		if (families != null) {
			for (String family : families) {
				if (this.getColumnFamily(family) == null)
					throw new IllegalArgumentException("Unknown column family " + family + " in class " + this.getClass());
				toBeActivated.add(family);
			}
		}

		this.getProperties();
		for (ColumnFamily<?> cf : this.getColumnFamilies()) {
			if (! toBeActivated.contains(cf.getName())) {
				Field f = cf.getProperty();
				ImplicitActivation ia = f == null ? null : f.getAnnotation(ImplicitActivation.class);
				if (f == null || ia != null) {
					toBeActivated.add(cf.getName());
				}
			}
		}
		
		Map<String, Map<String, byte[]>> rawData = this.getStore().get(this.getTable(), this.getIdentifier(), toBeActivated);
		for (String family : rawData.keySet()) {
			this.getColumnFamily(family).rebuild(rawData.get(family));
			boolean removed = toBeActivated.remove(family);
			assert removed;
		}
		
		if (!toBeActivated.isEmpty()) {
			Map<String, byte[]> emptyTree = new TreeMap<String, byte[]>();
			for (String tba : toBeActivated) {
				this.getColumnFamily(tba).rebuild(emptyTree);
			}
		}
		
		this.upgradeProperties();
	}
	
	public boolean PersistingElement.existsInStore() throws DatabaseNotReachedException {
		boolean ret = this.getStore().exists(this.getTable(), this.getIdentifier());
		this.exists = ret ? Boolean.TRUE : Boolean.FALSE;
		return ret;
	}
	
	public static <T> T getElement(Class<T> clazz, String identifier) {
		return KeyManagement.getInstance().createElement(clazz, identifier);
	}
	
	public static <T extends PersistingElement> Set<T> findElement(Class<T> clazz, Constraint c, int limit) throws DatabaseNotReachedException {
		Store store = StoreSelector.getInstance().getStoreFor(clazz);
		CloseableKeyIterator keys = null;
		try {
			keys = store.get(PersistingMixin.getInstance().getTable(clazz), c, limit);
			Set<T> ret = new TreeSet<T>();
			int count = 0;
			while(keys.hasNext()) {
				ret.add(ConversionTools.convertFromString(clazz, keys.next()));
				count++;
				if (count >= limit)
					break;
			}
			return ret;
		} finally {
			if (keys != null)
				keys.close();
		}
	}
	
	public static ConstraintBuilder findElements() {
		return new ConstraintBuilder();
	}
}
