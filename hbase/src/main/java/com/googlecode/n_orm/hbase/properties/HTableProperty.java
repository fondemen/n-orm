package com.googlecode.n_orm.hbase.properties;

import org.apache.hadoop.hbase.HTableDescriptor;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.Store;

/**
 * Property reader/writer to handle HBase table properties.
 * @param <T> the type of the property
 */
public abstract class HTableProperty<T> extends
		HBaseProperty<T> {
	/**
	 * Whether table has the expected value.
	 */
	abstract boolean hasValue(T value, HTableDescriptor table);

	/**
	 * Whether table has the expected value according to
	 * its {@link HBaseSchema annotation} or store.
	 */
	public boolean hasValue(HTableDescriptor table, Store store,
			Class<? extends PersistingElement> clazz,
			String tablePostfix) {
		T value = this.getValue(store, clazz, null, tablePostfix);
		return this.isSet(value) ? this.hasValue(value, table) : true;
	}

	/**
	 * Sets the value to the table descriptor (not altering).
	 */
	public abstract void setValue(T value, HTableDescriptor table);

	/**
	 * Sets the value to the table descriptor (not altering) according to
	 * its {@link HBaseSchema annotation} or store if defined.
	 */
	public void setValue(HTableDescriptor table, Store store,
			Class<? extends PersistingElement> clazz,
			String tablePostfix) {
		T value = this.getValue(store, clazz, null, tablePostfix);
		if (this.isSet(value)) {
			this.setValue(value, table);
		}
	}

	/**
	 * Checks whether this table descriptor should be altered.
	 */
	public boolean shouldAlter(HTableDescriptor table, Store store,
			Class<? extends PersistingElement> clazz,
			String tablePostfix) {
		T value = this.getValue(store, clazz, null, tablePostfix);
		return this.isSet(value) && !this.hasValue(value, table);
	}

}