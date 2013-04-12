package com.googlecode.n_orm.hbase.properties;

import org.apache.hadoop.hbase.HColumnDescriptor;

import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.Store;

class BlockSizeProperty extends ForcableHColumnProperty<Integer> {
	static class BlockSizeForcedProperty extends ShouldForceHBaseProperty {
		@Override
		HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann) {
			return ann.forceBlockSize();
		}

		@Override
		Boolean getDefaultValue(Store store) {
			return store.isForceBlockSize();
		}

		@Override
		String getName() {
			return "force block size";
		}

	}

	public BlockSizeProperty() {
		super(new BlockSizeForcedProperty());
	}

	@Override
	boolean hasValue(Integer value, HColumnDescriptor cf) {
		return cf.getBlocksize() == value.intValue();
	}

	@Override
	public void setValue(Integer value, HColumnDescriptor cf) {
		cf.setBlocksize(value);
	}

	@Override
	Integer readValue(HBaseSchema ann) {
		return ann.blockSize();
	}

	@Override
	Integer getDefaultValue(Store store) {
		return store.getBlockSize();
	}

	@Override
	boolean isSet(Integer value) {
		return value != null && value > 0;
	}

	@Override
	String getName() {
		return "block size";
	}
}