package com.googlecode.n_orm.hbase.properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.Store;

class CompressorProperty extends ForcableHColumnProperty<Algorithm> {
	static class CompressorForcedProperty extends ShouldForceHBaseProperty {
		@Override
		HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann) {
			return ann.forceCompression();
		}

		@Override
		Boolean getDefaultValue(Store store) {
			return store.isForceCompression();
		}

		@Override
		String getName() {
			return "force compression";
		}

	}

	public CompressorProperty() {
		super(new CompressorForcedProperty());
	}

	@Override
	boolean hasValue(Algorithm value, HColumnDescriptor cf) {
		return cf.getCompressionType().equals(value);
	}

	@Override
	public void setValue(Algorithm value, HColumnDescriptor cf) {
		cf.setCompressionType(value);
	}

	@Override
	Algorithm readValue(HBaseSchema ann) {
		return Store.getCompressionByName(ann.compression());
	}

	@Override
	Algorithm getDefaultValue(Store store) {
		return store.getCompressionAlgorithm();
	}

	@Override
	boolean isSet(Algorithm value) {
		return value != null;
	}

	@Override
	String getName() {
		return "compression";
	}
}