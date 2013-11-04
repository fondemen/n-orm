package com.googlecode.n_orm.hbase.properties;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.Store;

public class PropertyUtils {
	/**
	 * Set of HBase-schema properties available on tables.
	 */
	public static final HTableProperty<?>[] tableProperties;

	/**
	 * Set of HBase-schema properties available on column families.
	 */
	public static final HColumnFamilyProperty<?>[] columnProperties;

	static {
		tableProperties = new HTableProperty<?>[] {new DeferredLogFlushProperty()};
		
		columnProperties = new HColumnFamilyProperty[] { new InMemoryProperty(),
				new CompressorProperty(), new TTLProperty(),
				new MaxVersionsProperty(), new BloomTypeProperty(),
				new BlockCacheProperty(), new BlockSizeProperty(),
				new ReplicationScopeProperty() };
	}
	
	/**
	 * Checks whether given table descriptor is as expected (no testing column families).
	 */
	public static boolean asExpected(Store store, HTableDescriptor tableDescriptor, Class<? extends PersistingElement> clazz, String tablePostfix) {
		if (tableDescriptor == null) {
			return false;
		} else {
			for (HTableProperty<?> hprop : PropertyUtils.tableProperties) {
				if(hprop.shouldAlter(tableDescriptor, store, clazz, tablePostfix)) {
					return false;
				}
			}
			return true;
		}
	}
	
	/**
	 * Checks whether given table descriptor is as expected (no testing column families).
	 * @return list of properties badly set
	 */
	public static HTableProperty<?>[] checkIsAsExpected(Store store, HTableDescriptor tableDescriptor, Class<? extends PersistingElement> clazz, String tablePostfix) {
		if (tableDescriptor == null) {
			return null;
		} else {
			Collection<HTableProperty<?>> ret = new LinkedList<HTableProperty<?>>(Arrays.asList(PropertyUtils.tableProperties));
			Iterator<HTableProperty<?>> it = ret.iterator();
			while(it.hasNext()) {
				if (!it.next().shouldAlter(tableDescriptor, store, clazz, tablePostfix))
					it.remove();
			}
			return ret.toArray(new HTableProperty[ret.size()]);
		}
	}
	
	/**
	 * Changes given table descriptor so that it becomes as expected.
	 */
	public static void setValues(Store store, HTableDescriptor tableDescriptor, Class<? extends PersistingElement> clazz, String tablePostfix) {
		for (HTableProperty<?> hprop : PropertyUtils.tableProperties) {
			hprop.setValue(tableDescriptor, store, clazz, tablePostfix);
		}
	}

	/**
	 * Checks whether given column family descriptor is as expected.
	 */
	public static boolean asExpected(Store store, HColumnDescriptor columnDescriptor, Class<? extends PersistingElement> clazz, Field cfField, String tablePostfix) {
		if (columnDescriptor == null) {
			return false;
		} else {
			for (HColumnFamilyProperty<?> hprop : PropertyUtils.columnProperties) {
				if(hprop.shouldAlter(columnDescriptor, store, clazz, cfField, tablePostfix)) {
					return false;
				}
			}
			return true;
		}
	}
	
	/**
	 * Checks whether given table descriptor is as expected (no testing column families).
	 * @return list of properties badly set
	 */
	public static HColumnFamilyProperty<?>[] checkIsAsExpected(Store store, HColumnDescriptor columnDescriptor, Class<? extends PersistingElement> clazz, Field cfField, String tablePostfix) {
		if (columnDescriptor == null) {
			return null;
		} else {
			Collection<HColumnFamilyProperty<?>> ret = new LinkedList<HColumnFamilyProperty<?>>(Arrays.asList(PropertyUtils.columnProperties));
			Iterator<HColumnFamilyProperty<?>> it = ret.iterator();
			while(it.hasNext()) {
				if (!it.next().shouldAlter(columnDescriptor, store, clazz, cfField, tablePostfix))
					it.remove();
			}
			return ret.toArray(new HColumnFamilyProperty[ret.size()]);
		}
	}
	
	/**
	 * Changes given column family descriptor so that it becomes as expected.
	 */
	public static void setValues(Store store, HColumnDescriptor columnDescriptor, Class<? extends PersistingElement> clazz, Field cfField, String tablePostfix) {
		for (HColumnFamilyProperty<?> hprop : PropertyUtils.columnProperties) {
			hprop.setValue(columnDescriptor, store, clazz, cfField, tablePostfix);
		}
	}

	static final Map<PropertyCacheKey, PropertyCacheValue> values = new ConcurrentHashMap<PropertyCacheKey, PropertyCacheValue>();

	public static void clearCachedValues() {
		values.clear();
	}

	static Map<TypeWithPostfix, HBaseSchema> specificities = new TreeMap<TypeWithPostfix, HBaseSchema>();

	/**
	 * Resets caches ; for test purpose.
	 */
	public static void clearAllSchemaSpecificities() {
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
}
