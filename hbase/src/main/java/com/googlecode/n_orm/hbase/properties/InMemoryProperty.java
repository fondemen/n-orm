package com.googlecode.n_orm.hbase.properties;

import org.apache.hadoop.hbase.HColumnDescriptor;

import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.Store;

class InMemoryProperty extends ForcableHColumnProperty<Boolean> {
	static class InMemoryForcedProperty extends ShouldForceHBaseProperty {
		@Override
		HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann) {
			return ann.forceInMemory();
		}

		@Override
		Boolean getDefaultValue(Store store) {
			return store.isForceInMemory();
		}

		@Override
		String getName() {
			return "force inMemory";
		}

	}

	public InMemoryProperty() {
		super(new InMemoryForcedProperty());
	}

	@Override
	boolean hasValue(Boolean value, HColumnDescriptor cf) {
		return cf.isInMemory() == value.booleanValue();
	}

	@Override
	public void setValue(Boolean value, HColumnDescriptor cf) {
		cf.setInMemory(value);
	}

	@Override
	Boolean readValue(HBaseSchema ann) {
		return ShouldForceHBaseProperty.getBoolean(ann.inMemory());
	}

	@Override
	Boolean getDefaultValue(Store store) {
		return store.isInMemory();
	}

	@Override
	boolean isSet(Boolean value) {
		return value != null;
	}

	@Override
	String getName() {
		return "inMemory";
	}
}