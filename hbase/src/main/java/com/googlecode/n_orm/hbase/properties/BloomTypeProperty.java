package com.googlecode.n_orm.hbase.properties;

import java.util.logging.Level;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.Store;

class BloomTypeProperty extends ForcableHColumnProperty<StoreFile.BloomType> {
	static class BloomTypeForcedProperty extends ShouldForceHBaseProperty {
		@Override
		HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann) {
			return ann.forceBloomFilterType();
		}

		@Override
		Boolean getDefaultValue(Store store) {
			return store.isForceBloomFilterType();
		}

		@Override
		String getName() {
			return "force bloom filter type";
		}

	}

	public BloomTypeProperty() {
		super(new BloomTypeForcedProperty());
	}

	@Override
	boolean hasValue(StoreFile.BloomType value, HColumnDescriptor cf) {
		return cf.getBloomFilterType().equals(value);
	}

	@Override
	public void setValue(StoreFile.BloomType value, HColumnDescriptor cf) {
		cf.setBloomFilterType(value);
	}

	@Override
	StoreFile.BloomType readValue(HBaseSchema ann) {
		String name = ann.bloomFilterType();
		if (name == null)
			return null;
		name = name.trim();
		if (name.length() == 0)
			return null;
		try {
			return StoreFile.BloomType.valueOf(ann.bloomFilterType().trim());
		} catch (Exception x) {
			Store.errorLogger.log(Level.WARNING, "Unknown bloom type: " + name,
					x);
			return null;
		}
	}

	@Override
	StoreFile.BloomType getDefaultValue(Store store) {
		return store.getBloomFilterType();
	}

	@Override
	boolean isSet(StoreFile.BloomType value) {
		return value != null;
	}

	@Override
	String getName() {
		return "max versions";
	}
}