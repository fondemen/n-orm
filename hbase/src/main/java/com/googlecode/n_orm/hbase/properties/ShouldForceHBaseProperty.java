package com.googlecode.n_orm.hbase.properties;

import com.googlecode.n_orm.hbase.HBaseSchema;

/**
 * Helper to handle forceXXX properties.
 */
abstract class ShouldForceHBaseProperty extends
		HBaseProperty<Boolean> {
	static Boolean getBoolean(HBaseSchema.SettableBoolean value) {
		switch (value) {
		case UNSET:
			return null;
		case FALSE:
			return Boolean.FALSE;
		case TRUE:
			return Boolean.TRUE;
		default:
			return null;
		}
	}

	@Override
	boolean isSet(Boolean value) {
		return value != null;
	}

	abstract HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann);

	@Override
	Boolean readValue(HBaseSchema ann) {
		return getBoolean(this.readRawValue(ann));
	}
}