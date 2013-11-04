package com.googlecode.n_orm.hbase.properties;

import org.apache.hadoop.hbase.HColumnDescriptor;

import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.Store;

class TTLProperty extends ForcableHColumnProperty<Integer> {
	static class TTLForcedProperty extends ShouldForceHBaseProperty {
		@Override
		HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann) {
			return ann.forceTimeToLive();
		}

		@Override
		Boolean getDefaultValue(Store store) {
			return store.isForceTimeToLive();
		}

		@Override
		String getName() {
			return "force TTL";
		}

	}

	public TTLProperty() {
		super(new TTLForcedProperty());
	}

	@Override
	boolean hasValue(Integer value, HColumnDescriptor cf) {
		return cf.getTimeToLive() == value.intValue();
	}

	@Override
	public void setValue(Integer value, HColumnDescriptor cf) {
		cf.setTimeToLive(value);
	}

	@Override
	Integer readValue(HBaseSchema ann) {
		return ann.timeToLiveInSeconds();
	}

	@Override
	Integer getDefaultValue(Store store) {
		return store.getTimeToLiveSeconds();
	}

	@Override
	boolean isSet(Integer value) {
		return value != null && value > 0;
	}

	@Override
	String getName() {
		return "TTL (in seconds)";
	}
}