package com.googlecode.n_orm.hbase.properties;

import org.apache.hadoop.hbase.HTableDescriptor;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.hbase.Store;

/**
 * A column family property handler accompanied with
 * a {@link ShouldForceHBaseProperty}.
 * @param <T> the type of the forced property
 */
abstract class ForcableHTableProperty<T> extends
		HTableProperty<T> {
	private ShouldForceHBaseProperty forcedProperty;

	public ForcableHTableProperty(ShouldForceHBaseProperty forcedProperty) {
		this.forcedProperty = forcedProperty;
	}

	/**
	 * Alters properties of the given table descriptor
	 * if different and accompanying forced property states so.
	 */
	@Override
	public boolean shouldAlter(HTableDescriptor table, Store store,
			Class<? extends PersistingElement> clazz,
			String tablePostfix) {
		Boolean shouldForce = this.forcedProperty.getValue(store, clazz,
				null, tablePostfix);
		return this.forcedProperty.isSet(shouldForce)
				&& Boolean.TRUE.equals(shouldForce)
				&& super.shouldAlter(table, store, clazz, tablePostfix);
	}
}