package com.googlecode.n_orm.hbase.properties;

import org.apache.hadoop.hbase.HTableDescriptor;

import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.Store;

class DeferredLogFlushProperty extends ForcableHTableProperty<Boolean> {
	static class DeferredLogFlushForcedProperty extends
			ShouldForceHBaseProperty {
		@Override
		HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann) {
			return ann.forceDeferredLogFlush();
		}

		@Override
		Boolean getDefaultValue(Store store) {
			return store.isForceDeferredLogFlush();
		}

		@Override
		String getName() {
			return "force deferred log flush";
		}

	}

	public DeferredLogFlushProperty() {
		super(new DeferredLogFlushForcedProperty());
	}

	@Override
	boolean hasValue(Boolean value, HTableDescriptor table) {
		return table.isDeferredLogFlush() == value.booleanValue();
	}

	@Override
	public void setValue(Boolean value, HTableDescriptor table) {
		table.setDeferredLogFlush(value);
	}

	@Override
	Boolean readValue(HBaseSchema ann) {
		return ShouldForceHBaseProperty.getBoolean(ann.deferredLogFlush());
	}

	@Override
	Boolean getDefaultValue(Store store) {
		return store.getDeferredLogFlush();
	}

	@Override
	boolean isSet(Boolean value) {
		return value != null;
	}

	@Override
	String getName() {
		return "deferred log flush";
	}
}