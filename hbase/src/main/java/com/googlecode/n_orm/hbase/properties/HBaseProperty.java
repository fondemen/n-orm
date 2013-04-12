package com.googlecode.n_orm.hbase.properties;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.Store;

/**
 * A property handler to manage HBase schemas.
 * @param <T> the type of the handled property.
 */
public abstract class HBaseProperty<T> {
	
	/**
	 * The identifying name of the property.
	 */
	abstract String getName();

	/**
	 * Reads the value of the property as defined by annotation.
	 */
	abstract T readValue(HBaseSchema ann);

	/**
	 * The value of the property as prescribed by the store
	 * (could be configured by {@link StorageManagement store.properties}).
	 */
	abstract T getDefaultValue(Store store);

	/**
	 * Whether the obtained value for the property represents a set property.
	 */
	abstract boolean isSet(T value);

	/**
	 * Gets the value for this property for the given field, class, or store
	 * (in that order of preference). Result is cached in
	 * {@link PropertyUtils#values}.
	 * 
	 * @return null if unset
	 */
	@SuppressWarnings("unchecked")
	final T getValue(Store store, Class<? extends PersistingElement> clazz,
			Field field, String tablePostfix) {
		PropertyCacheKey cacheKey = new PropertyCacheKey(clazz, field,
				tablePostfix);

		PropertyCacheValue cacheVal = PropertyUtils.values.get(cacheKey);
		if (cacheVal == null) {
			cacheVal = new PropertyCacheValue();
			PropertyUtils.values.put(cacheKey, cacheVal);
		}

		return (T) cacheVal.getValue(this, store, clazz, field,
				tablePostfix);
	}

	/**
	 * Actual method that determines the value for this property according
	 * to field, class and store (in that order). This method is used by
	 * {@link PropertyCacheValue values} of the {@link PropertyUtils#values
	 * property cache} when not set.
	 * 
	 * @return null if unset
	 */
	T getValueInt(Store store,
			Class<? extends PersistingElement> clazz, Field field,
			String tablePostfix) {
		List<HBaseSchema> possibleSchemas = new LinkedList<HBaseSchema>();
		HBaseSchema tmp;

		if (field != null) {
			if (tablePostfix != null) {
				tmp = PropertyUtils.specificities.get(new ColumnFamilyWithPostfix(field,
						tablePostfix));
				if (tmp != null)
					possibleSchemas.add(tmp);
			}

			tmp = field.getAnnotation(HBaseSchema.class);
			if (tmp != null)
				possibleSchemas.add(tmp);
		}

		if (clazz != null) {
			if (tablePostfix != null) {
				tmp = PropertyUtils.specificities.get(new ClassWithPostfix(clazz,
						tablePostfix));
				if (tmp != null)
					possibleSchemas.add(tmp);
			}

			tmp = clazz.getAnnotation(HBaseSchema.class);
			if (tmp != null)
				possibleSchemas.add(tmp);
		}

		for (HBaseSchema schemaSpecificities : possibleSchemas) {
			if (schemaSpecificities != null) {
				T ret = this.readValue(schemaSpecificities);
				if (this.isSet(ret))
					return ret;
			}
		}

		return this.getDefaultValue(store);
	}

	@Override
	public String toString() {
		return this.getName();
	}
}