package com.googlecode.n_orm.hbase.properties;

import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.hbase.Store;

// A class to be used as the value for properties cache
class PropertyCacheValue {
	// The value representing NULL to avoid double checking get and contains in the cache
	private static final Object NULL_VALUE = new Object();
	
	// Comparator for cache sorted map
	private static final Comparator<HBaseProperty<?>> comparator = new Comparator<HBaseProperty<?>>() {

		@Override
		public int compare(HBaseProperty<?> o1, HBaseProperty<?> o2) {
			return o2.hashCode() - o1.hashCode();
		}
	};
	
	// The cache for values
	private volatile SortedMap<HBaseProperty<?>, Object> values = new TreeMap<HBaseProperty<?>, Object>(comparator);

	public Object getValue(HBaseProperty<?> property, Store store,
			Class<? extends PersistingElement> clazz, Field field,
			String tablePostfix) {
		// First checking cache (should not return null, but NULL_VALUE)
		Object ret = this.values.get(property);
		if (ret == null) {
			// Value does not exists
			// Getting it and caching it
			// Added to cache using copy-on-write
			synchronized(this) {
				// Double check in synchronized section
				ret = this.values.get(property);
				if (ret == null) {
					// Yes, we really have to get that value
					ret = property.getValueInt(store, clazz, field, tablePostfix);
					// Not caching null value so that get immediately tells whether value exists in cache
					if (ret == null)
						ret = NULL_VALUE;
					// Copy of cache
					SortedMap<HBaseProperty<?>, Object> newValues = new TreeMap<HBaseProperty<?>, Object>(values);
					// Adding value to cache
					newValues.put(property, ret);
					// Copy-on-write
					this.values = newValues;
				}
			}
		}
		return ret == NULL_VALUE ? null : ret;
	}
}