package com.googlecode.n_orm.hbase;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;

public class PropertyUtils {
	public static final HColumnFamilyProperty<?>[] properties;

	static {
		properties = new HColumnFamilyProperty[] { new InMemoryProperty(),
				new CompressorProperty(), new TTLProperty(),
				new MaxVersionsProperty(), new BloomTypeProperty(),
				new BlockCacheProperty(), new BlockSizeProperty(),
				new ReplicationScopeProperty() };
	}

	// A class to be used as the key for properties cache
	private static class PropertyCacheKey {
		private final int hashCode;
		private final Class<? extends PersistingElement> clazz;
		private final Field field;
		private final String postfix;

		private PropertyCacheKey(Class<? extends PersistingElement> clazz,
				Field field, String postfix) {
			super();
			this.clazz = clazz;
			this.field = field;
			this.postfix = postfix;
			final int prime = 31;
			int result = 1;
			result = prime * result + ((clazz == null) ? 0 : clazz.hashCode());
			result = prime * result + ((field == null) ? 0 : field.hashCode());
			result = prime * result
					+ ((postfix == null) ? 0 : postfix.hashCode());
			this.hashCode = result;
		}

		@Override
		public int hashCode() {
			return this.hashCode;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PropertyCacheKey other = (PropertyCacheKey) obj;
			if (hashCode != other.hashCode)
				return false;
			if (clazz == null) {
				if (other.clazz != null)
					return false;
			} else if (!clazz.equals(other.clazz))
				return false;
			if (field == null) {
				if (other.field != null)
					return false;
			} else if (!field.equals(other.field))
				return false;
			if (postfix == null) {
				if (other.postfix != null)
					return false;
			} else if (!postfix.equals(other.postfix))
				return false;
			return true;
		}
	}

	// A class to be used as the value for properties cache
	private static class PropertyCacheValue {
		private final Map<HBaseProperty<?>, Object> values = new TreeMap<HBaseProperty<?>, Object>(
				new Comparator<HBaseProperty<?>>() {

					@Override
					public int compare(HBaseProperty<?> o1, HBaseProperty<?> o2) {
						return o1.getName().compareTo(o2.getName());
					}
				});

		public Object getValue(HBaseProperty<?> property, Store store,
				Class<? extends PersistingElement> clazz, Field field,
				String tablePostfix) {
			Object ret = this.values.get(property);
			if (ret == null && !this.values.containsKey(property)) {
				ret = property.getValueInt(store, clazz, field, tablePostfix);
				this.values.put(property, ret);
			}
			return ret;
		}
	}

	private static final Map<PropertyCacheKey, PropertyCacheValue> values = new ConcurrentHashMap<PropertyCacheKey, PropertyCacheValue>();

	static void clearCachedValues() {
		values.clear();
	}

	private static abstract class TypeWithPostfix implements
			Comparable<TypeWithPostfix> {
		private final String postfix;

		public TypeWithPostfix(String postfix) {
			this.postfix = postfix;
		}

		public String getPostfix() {
			return postfix;
		}

		@Override
		public int compareTo(TypeWithPostfix o) {
			if (!this.getClass().equals(o.getClass()))
				return this.getClass().getSimpleName()
						.compareTo(o.getClass().getName());
			return this.getPostfix().compareTo(o.getPostfix());
		}
	}

	private static class ClassWithPostfix extends TypeWithPostfix {
		private final Class<? extends PersistingElement> clazz;

		private ClassWithPostfix(Class<? extends PersistingElement> clazz,
				String postfix) {
			super(postfix);
			this.clazz = clazz;
		}

		public Class<? extends PersistingElement> getClazz() {
			return clazz;
		}

		@Override
		public int compareTo(TypeWithPostfix o) {
			int ret = super.compareTo(o);
			if (ret != 0)
				return ret;
			return this
					.getClazz()
					.getSimpleName()
					.compareTo(
							((ClassWithPostfix) o).getClazz().getSimpleName());
		}

	}

	private static class ColumnFamilyWithPostfix extends TypeWithPostfix {
		private final Field field;

		private ColumnFamilyWithPostfix(Field field, String postfix) {
			super(postfix);
			this.field = field;
		}

		public Field getField() {
			return field;
		}

		@Override
		public int compareTo(TypeWithPostfix o) {
			int ret = super.compareTo(o);
			if (ret != 0)
				return ret;
			return this
					.getField()
					.getName()
					.compareTo(
							((ColumnFamilyWithPostfix) o).getField().getName());
		}

	}

	@HBaseSchema
	private static class DummyClass {
	}

	public static class DefaultHBaseSchema implements HBaseSchema {
		private static HBaseSchema defaultHbaseSchema;

		static {
			defaultHbaseSchema = DummyClass.class
					.getAnnotation(HBaseSchema.class);
		}

		private Class<? extends Annotation> annotationType = defaultHbaseSchema
				.annotationType();
		private SettableBoolean forceCompression = defaultHbaseSchema
				.forceCompression();
		private String compression = defaultHbaseSchema.compression();
		private int scanCaching = defaultHbaseSchema.scanCaching();
		private SettableBoolean forceInMemory = defaultHbaseSchema
				.forceInMemory();
		private SettableBoolean inMemory = defaultHbaseSchema.inMemory();
		private SettableBoolean forceTimeToLive = defaultHbaseSchema
				.forceTimeToLive();
		private int timeToLiveInSeconds = defaultHbaseSchema
				.timeToLiveInSeconds();
		private SettableBoolean forceMaxVersions = defaultHbaseSchema
				.forceMaxVersions();
		private int maxVersions = defaultHbaseSchema.maxVersions();
		private SettableBoolean forceBloomFilterType = defaultHbaseSchema
				.forceBloomFilterType();
		private String bloomFilterType = defaultHbaseSchema.bloomFilterType();
		private SettableBoolean forceBlockCacheEnabled = defaultHbaseSchema
				.forceBlockCacheEnabled();
		private SettableBoolean blockCacheEnabled = defaultHbaseSchema
				.blockCacheEnabled();
		private SettableBoolean forceBlockSize = defaultHbaseSchema
				.forceBlockSize();
		private int blockSize = defaultHbaseSchema.blockSize();
		private SettableBoolean forceReplicationScope = defaultHbaseSchema
				.forceReplicationScope();
		private int replicationScope = defaultHbaseSchema.replicationScope();

		@Override
		public Class<? extends Annotation> annotationType() {
			return this.annotationType;
		}

		@Override
		public SettableBoolean forceCompression() {
			return this.forceCompression;
		}

		@Override
		public String compression() {
			return this.compression;
		}

		@Override
		public int scanCaching() {
			return this.scanCaching;
		}

		@Override
		public SettableBoolean forceInMemory() {
			return this.forceInMemory;
		}

		@Override
		public SettableBoolean inMemory() {
			return this.inMemory;
		}

		@Override
		public SettableBoolean forceTimeToLive() {
			return this.forceTimeToLive;
		}

		@Override
		public int timeToLiveInSeconds() {
			return this.timeToLiveInSeconds;
		}

		@Override
		public SettableBoolean forceMaxVersions() {
			return this.forceMaxVersions;
		}

		@Override
		public int maxVersions() {
			return this.maxVersions;
		}

		@Override
		public SettableBoolean forceBloomFilterType() {
			return this.forceBloomFilterType;
		}

		@Override
		public String bloomFilterType() {
			return this.bloomFilterType;
		}

		@Override
		public SettableBoolean forceBlockCacheEnabled() {
			return this.forceBlockCacheEnabled;
		}

		@Override
		public SettableBoolean blockCacheEnabled() {
			return this.blockCacheEnabled;
		}

		@Override
		public SettableBoolean forceBlockSize() {
			return this.forceBlockSize;
		}

		@Override
		public int blockSize() {
			return this.blockSize;
		}

		@Override
		public SettableBoolean forceReplicationScope() {
			return this.forceReplicationScope;
		}

		@Override
		public int replicationScope() {
			return this.replicationScope;
		}

		public void setAnnotationType(Class<? extends Annotation> annotationType) {
			this.annotationType = annotationType;
		}

		public void setForceCompression(SettableBoolean forceCompression) {
			this.forceCompression = forceCompression;
		}

		public void setCompression(String compression) {
			this.compression = compression;
		}

		public void setScanCaching(int scanCaching) {
			this.scanCaching = scanCaching;
		}

		public void setForceInMemory(SettableBoolean forceInMemory) {
			this.forceInMemory = forceInMemory;
		}

		public void setInMemory(SettableBoolean inMemory) {
			this.inMemory = inMemory;
		}

		public void setForceTimeToLive(SettableBoolean forceTimeToLive) {
			this.forceTimeToLive = forceTimeToLive;
		}

		public void setTimeToLiveInSeconds(int timeToLiveInSeconds) {
			this.timeToLiveInSeconds = timeToLiveInSeconds;
		}

		public void setForceMaxVersions(SettableBoolean forceMaxVersions) {
			this.forceMaxVersions = forceMaxVersions;
		}

		public void setMaxVersions(int maxVersions) {
			this.maxVersions = maxVersions;
		}

		public void setForceBloomFilterType(SettableBoolean forceBloomFilterType) {
			this.forceBloomFilterType = forceBloomFilterType;
		}

		public void setBloomFilterType(String bloomFilterType) {
			this.bloomFilterType = bloomFilterType;
		}

		public void setForceBlockCacheEnabled(
				SettableBoolean forceBlockCacheEnabled) {
			this.forceBlockCacheEnabled = forceBlockCacheEnabled;
		}

		public void setBlockCacheEnabled(SettableBoolean blockCacheEnabled) {
			this.blockCacheEnabled = blockCacheEnabled;
		}

		public void setForceBlockSize(SettableBoolean forceBlockSize) {
			this.forceBlockSize = forceBlockSize;
		}

		public void setBlockSize(int blockSize) {
			this.blockSize = blockSize;
		}

		public void setForceReplicationScope(
				SettableBoolean forceReplicationScope) {
			this.forceReplicationScope = forceReplicationScope;
		}

		public void setReplicationScope(int replicationScope) {
			this.replicationScope = replicationScope;
		}

	}

	private static Map<TypeWithPostfix, HBaseSchema> specificities = new TreeMap<TypeWithPostfix, HBaseSchema>();

	static void clearAllSchemaSpecificities() {
		specificities.clear();
		values.clear();
	}

	/**
	 * Provides a mean to override a schema specification for a specific table
	 * in case of a class to be persisted in a {@link Persisting#federated()
	 * federated table}. Please note that if a column family is defined using
	 * {@link HBaseSchema}, this definition is not overridden by overriding
	 * class using this method.
	 * 
	 * @param clazz
	 *            the class of the persisting elements
	 * @param tablePostfix
	 *            the postfix for the table for which which schema specificities
	 *            should be registered ; null means no postfix
	 * @param schema
	 *            the schema specificities ; can be a {@link DefaultHBaseSchema}
	 */
	public static void registerSchemaSpecificity(
			Class<? extends PersistingElement> clazz, String tablePostfix,
			HBaseSchema schema) {
		if (clazz == null)
			throw new NullPointerException();
		specificities.put(new ClassWithPostfix(clazz, tablePostfix), schema);
		values.clear();
	}

	/**
	 * Provides a mean to override a schema specification for a specific column
	 * family in case of a class to be persisted in a
	 * {@link Persisting#federated() federated table}.
	 * 
	 * @param field
	 *            the column family
	 * @param tablePostfix
	 *            the postfix for the table for which which schema specificities
	 *            should be registered ; null means no postfix
	 * @param schema
	 *            the schema specificities ; can be a {@link DefaultHBaseSchema}
	 */
	public static void registerSchemaSpecificity(Field field,
			String tablePostfix, HBaseSchema schema) {
		if (field == null)
			throw new NullPointerException();
		specificities.put(new ColumnFamilyWithPostfix(field, tablePostfix),
				schema);
		values.clear();
	}

	public static abstract class HBaseProperty<T> {
		abstract String getName();

		abstract T readValue(HBaseSchema ann);

		abstract T getDefaultValue(Store store);

		abstract boolean isSet(T value);

		/**
		 * Gets the value for this property for the given field, class, or store
		 * (in that order of preference). Result is cached in
		 * {@link PropertyUtils#values}.
		 * 
		 * @return null if unset
		 */
		@SuppressWarnings("unchecked")
		final T getValue(Store store, Class<? extends PersistingElement> clazz,
				Field field, String tablePostfix) {
			PropertyCacheKey cacheKey = new PropertyCacheKey(clazz, field,
					tablePostfix);

			PropertyCacheValue cacheVal = values.get(cacheKey);
			if (cacheVal == null) {
				cacheVal = new PropertyCacheValue();
				values.put(cacheKey, cacheVal);
			}

			return (T) cacheVal.getValue(this, store, clazz, field,
					tablePostfix);
		}

		/**
		 * Actual method that determines the value for this property according
		 * to field, class and store (in that order). This method is used by
		 * {@link PropertyCacheValue values} of the {@link PropertyUtils#values
		 * property cache} when not set.
		 * 
		 * @return null if unset
		 */
		private T getValueInt(Store store,
				Class<? extends PersistingElement> clazz, Field field,
				String tablePostfix) {
			List<HBaseSchema> possibleSchemas = new LinkedList<HBaseSchema>();
			HBaseSchema tmp;

			if (field != null) {
				if (tablePostfix != null) {
					tmp = specificities.get(new ColumnFamilyWithPostfix(field,
							tablePostfix));
					if (tmp != null)
						possibleSchemas.add(tmp);
				}

				tmp = field.getAnnotation(HBaseSchema.class);
				if (tmp != null)
					possibleSchemas.add(tmp);
			}

			if (clazz != null) {
				if (tablePostfix != null) {
					tmp = specificities.get(new ClassWithPostfix(clazz,
							tablePostfix));
					if (tmp != null)
						possibleSchemas.add(tmp);
				}

				tmp = clazz.getAnnotation(HBaseSchema.class);
				if (tmp != null)
					possibleSchemas.add(tmp);
			}

			for (HBaseSchema schemaSpecificities : possibleSchemas) {
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

	public static abstract class HColumnFamilyProperty<T> extends
			HBaseProperty<T> {
		abstract boolean hasValue(T value, HColumnDescriptor cf);

		public boolean hasValue(HColumnDescriptor cf, Store store,
				Class<? extends PersistingElement> clazz, Field field,
				String tablePostfix) {
			T value = this.getValue(store, clazz, field, tablePostfix);
			return this.isSet(value) ? this.hasValue(value, cf) : true;
		}

		public abstract void setValue(T value, HColumnDescriptor cf);

		public void setValue(HColumnDescriptor cf, Store store,
				Class<? extends PersistingElement> clazz, Field field,
				String tablePostfix) {
			T value = this.getValue(store, clazz, field, tablePostfix);
			if (this.isSet(value)) {
				this.setValue(value, cf);
			}
		}

		public boolean alter(HColumnDescriptor cf, Store store,
				Class<? extends PersistingElement> clazz, Field field,
				String tablePostfix) {
			T value = this.getValue(store, clazz, field, tablePostfix);
			if (this.isSet(value) && !this.hasValue(value, cf)) {
				this.setValue(value, cf);
				return true;
			} else {
				return false;
			}
		}

	}

	private static abstract class ShouldForceHBaseProperty extends
			HBaseProperty<Boolean> {
		static Boolean getBoolean(HBaseSchema.SettableBoolean value) {
			switch (value) {
			case UNSET:
				return null;
			case FALSE:
				return Boolean.FALSE;
			case TRUE:
				return Boolean.TRUE;
			default:
				return null;
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

	private static abstract class ForcableHBaseProperty<T> extends
			HColumnFamilyProperty<T> {
		private ShouldForceHBaseProperty forcedProperty;

		public ForcableHBaseProperty(ShouldForceHBaseProperty forcedProperty) {
			this.forcedProperty = forcedProperty;
		}

		@Override
		public boolean alter(HColumnDescriptor cf, Store store,
				Class<? extends PersistingElement> clazz, Field field,
				String tablePostfix) {
			Boolean shouldForce = this.forcedProperty.getValue(store, clazz,
					field, tablePostfix);
			if (this.forcedProperty.isSet(shouldForce)
					&& Boolean.TRUE.equals(shouldForce)) {
				return super.alter(cf, store, clazz, field, tablePostfix);
			} else {
				return false;
			}
		}
	}

	private static class CompressorForcedProperty extends
			ShouldForceHBaseProperty {
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

	private static class CompressorProperty extends
			ForcableHBaseProperty<Algorithm> {
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

	private static class InMemoryForcedProperty extends
			ShouldForceHBaseProperty {
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

	private static class InMemoryProperty extends
			ForcableHBaseProperty<Boolean> {
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

	private static class MaxVersionsForcedProperty extends
			ShouldForceHBaseProperty {
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

	private static class MaxVersionsProperty extends
			ForcableHBaseProperty<Integer> {
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

	private static class BloomTypeForcedProperty extends
			ShouldForceHBaseProperty {
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

	private static class BloomTypeProperty extends
			ForcableHBaseProperty<StoreFile.BloomType> {
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
				return StoreFile.BloomType
						.valueOf(ann.bloomFilterType().trim());
			} catch (Exception x) {
				Store.errorLogger.log(Level.WARNING, "Unknown bloom type: "
						+ name, x);
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

	private static class BlockCacheForcedProperty extends
			ShouldForceHBaseProperty {
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

	private static class BlockCacheProperty extends
			ForcableHBaseProperty<Boolean> {
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

	private static class BlockSizeForcedProperty extends
			ShouldForceHBaseProperty {
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

	private static class BlockSizeProperty extends
			ForcableHBaseProperty<Integer> {
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

	private static class ReplicationScopeForcedProperty extends
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

	private static class ReplicationScopeProperty extends
			ForcableHBaseProperty<Integer> {
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
