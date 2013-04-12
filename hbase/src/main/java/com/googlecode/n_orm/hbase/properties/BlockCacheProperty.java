package com.googlecode.n_orm.hbase.properties;

import org.apache.hadoop.hbase.HColumnDescriptor;

import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.Store;

class BlockCacheProperty extends ForcableHColumnProperty<Boolean> {
	static class BlockCacheForcedProperty extends ShouldForceHBaseProperty {
		@Override
		HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann) {
			return ann.forceBlockCacheEnabled();
		}

		@Override
		Boolean getDefaultValue(Store store) {
			return store.isForceBlockCacheEnabled();
		}

		@Override
		String getName() {
			return "force block cache";
		}

	}

	public BlockCacheProperty() {
		super(new BlockCacheForcedProperty());
	}

	@Override
	boolean hasValue(Boolean value, HColumnDescriptor cf) {
		return cf.isBlockCacheEnabled() == value.booleanValue();
	}

	@Override
	public void setValue(Boolean value, HColumnDescriptor cf) {
		cf.setBlockCacheEnabled(value);
	}

	@Override
	Boolean readValue(HBaseSchema ann) {
		return ShouldForceHBaseProperty.getBoolean(ann.blockCacheEnabled());
	}

	@Override
	Boolean getDefaultValue(Store store) {
		return store.getBlockCacheEnabled();
	}

	@Override
	boolean isSet(Boolean value) {
		return value != null;
	}

	@Override
	String getName() {
		return "block cache";
	}
}