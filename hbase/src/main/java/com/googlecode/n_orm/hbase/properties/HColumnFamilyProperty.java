package com.googlecode.n_orm.hbase.properties;

import java.lang.reflect.Field;

import org.apache.hadoop.hbase.HColumnDescriptor;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.Store;

/**
 * Property reader/writer to handle HBase column family properties.
 * @param <T> the type of the property
 */
public abstract class HColumnFamilyProperty<T> extends
		HBaseProperty<T> {
	/**
	 * Whether column family has the expected value.
	 */
	abstract boolean hasValue(T value, HColumnDescriptor cf);

	/**
	 * Whether column family has the expected value according to
	 * its {@link HBaseSchema annotation} or store.
	 */
	public boolean hasValue(HColumnDescriptor cf, Store store,
			Class<? extends PersistingElement> clazz, Field field,
			String tablePostfix) {
		T value = this.getValue(store, clazz, field, tablePostfix);
		return this.isSet(value) ? this.hasValue(value, cf) : true;
	}

	/**
	 * Sets the value to the column family descriptor (not altering).
	 */
	public abstract void setValue(T value, HColumnDescriptor cf);

	public void setValue(HColumnDescriptor cf, Store store,
			Class<? extends PersistingElement> clazz, Field field,
			String tablePostfix) {
		T value = this.getValue(store, clazz, field, tablePostfix);
		if (this.isSet(value)) {
			this.setValue(value, cf);
		}
	}


	/**
	 * Checks whether this table descriptor should be altered.
	 */
	public boolean shouldAlter(HColumnDescriptor cf, Store store,
			Class<? extends PersistingElement> clazz, Field field,
			String tablePostfix) {
		T value = this.getValue(store, clazz, field, tablePostfix);
		return this.isSet(value) && !this.hasValue(value, cf);
	}

}