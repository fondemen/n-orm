package com.googlecode.n_orm.hbase.properties;

import org.apache.hadoop.hbase.HColumnDescriptor;

import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.Store;

class MaxVersionsProperty extends ForcableHColumnProperty<Integer> {
	static class MaxVersionsForcedProperty extends ShouldForceHBaseProperty {
		@Override
		HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann) {
			return ann.forceMaxVersions();
		}

		@Override
		Boolean getDefaultValue(Store store) {
			return store.isForceMaxVersions();
		}

		@Override
		String getName() {
			return "force max versions";
		}

	}

	public MaxVersionsProperty() {
		super(new MaxVersionsForcedProperty());
	}

	@Override
	boolean hasValue(Integer value, HColumnDescriptor cf) {
		return cf.getMaxVersions() == value.intValue();
	}

	@Override
	public void setValue(Integer value, HColumnDescriptor cf) {
		cf.setMaxVersions(value);
	}

	@Override
	Integer readValue(HBaseSchema ann) {
		return ann.maxVersions();
	}

	@Override
	Integer getDefaultValue(Store store) {
		return store.getMaxVersions();
	}

	@Override
	boolean isSet(Integer value) {
		return value != null && value > 0;
	}

	@Override
	String getName() {
		return "max versions";
	}
}