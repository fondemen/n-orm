package com.mt.storage;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.mt.storage.conversion.ConversionTools;

public aspect StorageManagement {
	
	private transient boolean PersistingElement.isStoring = false;
	
	public void PersistingElement.delete() throws DatabaseNotReachedException {
		this.getStore().delete(this.getTable(), this.getIdentifier());
	}
	
	public void PersistingElement.store() throws DatabaseNotReachedException {
		synchronized(this) {
			if (this.isStoring)
				return;
			isStoring = true;
		}
		try {
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
					Set<String> familyDeleted = new HashSet<String>();
					for (String key : changedKeys) {
						if (family.wasDeleted(key))
							familyDeleted.add(key);
						else {
							//No need for auto-loading for it is a changed value
							familyChanges.put(key, ConversionTools.convert(family.get(key)));
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
			Map<String, byte[]> changedProperties = changed.get(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME);
			if (changedProperties == null) {
				changedProperties = new TreeMap<String, byte[]>();
				changed.put(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, changedProperties);
			}
			
			for (Field key : this.getKeys()) {
				try {
					changedProperties.put(key.getName(), ConversionTools.convert(PropertyManagement.aspectOf().readValue(this, key)));
				} catch (RuntimeException e) {
					throw e;
				} catch (Exception e) {
					throw new IllegalStateException("Cannot save object ; problem reading property : " + e.getMessage(), e);
				}
			}
			this.getStore().storeChanges(this.getTable(), this.getIdentifier(), changed, deleted, increments);
			for(ColumnFamily<?> family : families) {
				family.clearChanges();
			}
			
			//Store depending properties
			for (Field prop : PropertyManagement.aspectOf().getProperties(this.getClass())) {
				if (PropertyManagement.aspectOf().isPersistingPropertyType(prop.getType()) && !prop.isAnnotationPresent(ExplicitActivation.class)) {
					Object kVal = PropertyManagement.aspectOf().candideReadValue(this, prop);
					((PersistingElement)kVal).store();
				}
			}
		} finally {
			synchronized(this) {
				isStoring = false;
			}
		}
	}
	
	public boolean PersistingElement.existsInStore() throws DatabaseNotReachedException {
		return this.getStore().exists(this.getTable(), this.getIdentifier());
	}
	
	public static <T extends PersistingElement> Set<T> findElement(Class<T> clazz, Constraint c, int limit) throws DatabaseNotReachedException {
		Store store = StoreSelector.aspectOf().getStoreFor(clazz);
		CloseableKeyIterator keys = null;
		try {
			keys = store.get(PersistingMixin.aspectOf().getTable(clazz), c, limit);
			Set<T> ret = new HashSet<T>();
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
}
