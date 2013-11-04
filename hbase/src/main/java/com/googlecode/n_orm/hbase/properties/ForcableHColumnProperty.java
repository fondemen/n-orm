package com.googlecode.n_orm.hbase.properties;

import java.lang.reflect.Field;

import org.apache.hadoop.hbase.HColumnDescriptor;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.hbase.Store;

/**
 * A column family property handler accompanied with
 * a {@link ShouldForceHBaseProperty}.
 * @param <T> the type of the forced property
 */
abstract class ForcableHColumnProperty<T> extends
		HColumnFamilyProperty<T> {
	private ShouldForceHBaseProperty forcedProperty;

	public ForcableHColumnProperty(ShouldForceHBaseProperty forcedProperty) {
		this.forcedProperty = forcedProperty;
	}

	/**
	 * Alters properties of the given column family descriptor
	 * if different and accompanying forced property states so.
	 */
	@Override
	public boolean shouldAlter(HColumnDescriptor cf, Store store,
			Class<? extends PersistingElement> clazz, Field field,
			String tablePostfix) {
		Boolean shouldForce = this.forcedProperty.getValue(store, clazz,
				field, tablePostfix);
		return this.forcedProperty.isSet(shouldForce)
				&& Boolean.TRUE.equals(shouldForce)
				&& super.shouldAlter(cf, store, clazz, field, tablePostfix);
	}
}