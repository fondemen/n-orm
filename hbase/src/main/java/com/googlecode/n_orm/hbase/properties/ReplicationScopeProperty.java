package com.googlecode.n_orm.hbase.properties;

import org.apache.hadoop.hbase.HColumnDescriptor;

import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.Store;

class ReplicationScopeProperty extends ForcableHColumnProperty<Integer> {
	static class ReplicationScopeForcedProperty extends
			ShouldForceHBaseProperty {
		@Override
		HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann) {
			return ann.forceReplicationScope();
		}

		@Override
		Boolean getDefaultValue(Store store) {
			return store.isForceReplicationScope();
		}

		@Override
		String getName() {
			return "force replication scope";
		}

	}

	public ReplicationScopeProperty() {
		super(new ReplicationScopeForcedProperty());
	}

	@Override
	boolean hasValue(Integer value, HColumnDescriptor cf) {
		return cf.getScope() == value.intValue();
	}

	@Override
	public void setValue(Integer value, HColumnDescriptor cf) {
		cf.setScope(value);
	}

	@Override
	Integer readValue(HBaseSchema ann) {
		return ann.replicationScope();
	}

	@Override
	Integer getDefaultValue(Store store) {
		return store.getReplicationScope();
	}

	@Override
	boolean isSet(Integer value) {
		return value != null && value > 0 && value < 2;
	}

	@Override
	String getName() {
		return "replication scope";
	}
}