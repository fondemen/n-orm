package com.googlecode.n_orm.hbase;

import java.lang.reflect.Field;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import com.googlecode.n_orm.PersistingElement;

public class PropertyUtils {
	public static final HColumnFamilyProperty[] properties;
	
	static {
		properties = new HColumnFamilyProperty[] {
			new InMemoryProperty(),
			new CompressorProperty(),
			new TTLProperty(),
			new MaxVersionsProperty(),
			new BloomTypeProperty(),
			new BlockCacheProperty(),
			new BlockSizeProperty()
		};
	}

	public static abstract class HBaseProperty<T> {
		
		abstract String getName();
		
		abstract T readValue(HBaseSchema ann); 
		
		abstract T getDefaultValue(Store store);
		
		abstract boolean isSet(T value);
	
		/**
		 * @return null if unset
		 */
		T getValue(Store store, Class<? extends PersistingElement> clazz, Field field) {
			HBaseSchema classLevelSchemaSpecificities = clazz == null ? null : clazz.getAnnotation(HBaseSchema.class);
			HBaseSchema columnFamilyLevelSchemaSpecificities = field == null ? null : field.getAnnotation(HBaseSchema.class);
			
			for (HBaseSchema schemaSpecificities : new HBaseSchema[] {columnFamilyLevelSchemaSpecificities, classLevelSchemaSpecificities}) {
				if (schemaSpecificities != null) {
					T ret = this.readValue(schemaSpecificities);
					if (this.isSet(ret))
						return ret;
				}
			}
			
			return this.getDefaultValue(store);
		}
		
		@Override
		public String toString() {
			return this.getName();
		}
	}

	public static abstract class HColumnFamilyProperty<T> extends HBaseProperty<T> {
		abstract boolean hasValue(T value, HColumnDescriptor cf);
		
		public boolean hasValue(HColumnDescriptor cf, Store store, Class<? extends PersistingElement> clazz, Field field) {
			T value = this.getValue(store, clazz, field);
			return this.isSet(value) ? this.hasValue(value, cf) : true;
		}
		
		public abstract void setValue(T value, HColumnDescriptor cf);
		
		public void setValue(HColumnDescriptor cf, Store store, Class<? extends PersistingElement> clazz, Field field) {
			T value = this.getValue(store, clazz, field);
			if (this.isSet(value)) {
				this.setValue(value, cf);
			}
		}
		
		public boolean alter(HColumnDescriptor cf, Store store, Class<? extends PersistingElement> clazz, Field field) {
			T value = this.getValue(store, clazz, field);
			if (this.isSet(value) && !this.hasValue(value, cf)) {
				this.setValue(value, cf);
				return true;
			} else {
				return false;
			}
		}
		
	}

	private static abstract class ShouldForceHBaseProperty extends HBaseProperty<Boolean> {
		static Boolean getBoolean(HBaseSchema.SettableBoolean value) {
			switch (value) {
			case UNSET: return null;
			case FALSE: return Boolean.FALSE;
			case TRUE: return Boolean.TRUE;
			default: return null;
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

	private static abstract class ForcableHBaseProperty<T> extends HColumnFamilyProperty<T> {
		private ShouldForceHBaseProperty forcedProperty;
		
		public ForcableHBaseProperty(ShouldForceHBaseProperty forcedProperty) {
			this.forcedProperty = forcedProperty;
		}
		
		@Override
		public boolean alter(HColumnDescriptor cf, Store store,
				Class<? extends PersistingElement> clazz, Field field) {
			Boolean shouldForce = this.forcedProperty.getValue(store, clazz, field);
			if (this.forcedProperty.isSet(shouldForce) && Boolean.TRUE.equals(shouldForce)) {
				return super.alter(cf, store, clazz, field);
			} else {
				return false;
			}
		}
	}

	private static class CompressorForcedProperty extends ShouldForceHBaseProperty {
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

	private static class CompressorProperty extends ForcableHBaseProperty<Algorithm> {
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

	private static class InMemoryForcedProperty extends ShouldForceHBaseProperty {
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

	private static class InMemoryProperty extends ForcableHBaseProperty<Boolean> {
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

	private static class TTLForcedProperty extends ShouldForceHBaseProperty {
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

	private static class TTLProperty extends ForcableHBaseProperty<Integer> {
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


	private static class MaxVersionsForcedProperty extends ShouldForceHBaseProperty {
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

	private static class MaxVersionsProperty extends ForcableHBaseProperty<Integer> {
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


	private static class BloomTypeForcedProperty extends ShouldForceHBaseProperty {
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

	private static class BloomTypeProperty extends ForcableHBaseProperty<StoreFile.BloomType> {
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
			return ann.bloomFilterType();
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

	private static class BlockCacheForcedProperty extends ShouldForceHBaseProperty {
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

	private static class BlockCacheProperty extends ForcableHBaseProperty<Boolean> {
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

	private static class BlockSizeForcedProperty extends ShouldForceHBaseProperty {
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

	private static class BlockSizeProperty extends ForcableHBaseProperty<Integer> {
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

	private static class ReplicationScopeForcedProperty extends ShouldForceHBaseProperty {
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

	private static class ReplicationScopeProperty extends ForcableHBaseProperty<Integer> {
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
}
