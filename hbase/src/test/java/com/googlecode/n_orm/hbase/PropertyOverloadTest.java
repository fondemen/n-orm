package com.googlecode.n_orm.hbase;

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.FederatedMode;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingMixin;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.cf.SetColumnFamily;
import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.Store;
import com.googlecode.n_orm.hbase.HBaseSchema.SettableBoolean;
import com.googlecode.n_orm.hbase.properties.DefaultHBaseSchema;
import com.googlecode.n_orm.hbase.properties.PropertyUtils;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;

public class PropertyOverloadTest {
	private static Store store;
	private static final String table;
	private static String key = "YT78I6YJ890NF6B_ht-èit-";
	private static final String id = key + KeyManagement.KEY_SEPARATOR;
	private static String dummyQualifier = "78YGI8G67H90";
	private static byte[] dummyValue = Bytes.toBytes("787RC56Y0J9YYJ");
	private static final Element elt = new Element();
	private static final Field defaultCfField, overloadedCfField, dummyCfField;
	
	static {
		try {
			table = PersistingMixin.getInstance().getTable(Element.class);
			defaultCfField = Element.class.getDeclaredField("defaultCf");
			overloadedCfField = Element.class.getDeclaredField("overlodedCf");
			dummyCfField = Element.class.getDeclaredField("dummyCf");
		} catch (Exception x) {
			throw new RuntimeException(x);
		}
	}

	@BeforeClass
	public static void prepareStore() {
		HBaseLauncher.prepareHBase();
		store = HBaseLauncher.hbaseStore;
	}
	
	@Before
	public void resetStoreCache() {
		store.clearCache();
	}
	
	//Just a dummy class so that we've got something to give as parameter...
	@Persisting(table="PropertyOverloadTest", federated=FederatedMode.CONS)
	@HBaseSchema(compression="gz", forceInMemory=SettableBoolean.TRUE, inMemory=SettableBoolean.TRUE, scanCaching=1)
	public static class Element extends DummyPersistingElement {
		private transient String postfix = "";
		
		private static final long serialVersionUID = 1L;

		@Key public String key = PropertyOverloadTest.key;
		
		public SetColumnFamily<String> defaultCf = new SetColumnFamily<String>();
		@HBaseSchema(forceCompression=SettableBoolean.TRUE, inMemory=SettableBoolean.FALSE, bloomFilterType="ROW", blockCacheEnabled=SettableBoolean.FALSE, blockSize=1234, maxVersions=1, replicationScope=1, timeToLiveInSeconds=4321, scanCaching=2) public SetColumnFamily<String> overlodedCf = new SetColumnFamily<String>();
		@HBaseSchema(bloomFilterType="dummy", blockSize=-34, maxVersions=-12, replicationScope=2, timeToLiveInSeconds=-13, scanCaching=3) public SetColumnFamily<String> dummyCf = new SetColumnFamily<String>();
		
		public String getTablePostfix() {
			return this.postfix;
		}
	}
	
	public void deleteTable() {
		this.deleteTable("");
	}
	
	public void deleteTable(String postfix) {
		try {
			String actualTable = table+postfix;
			if (store.getAdmin().tableExists(actualTable)) {
				if (store.getAdmin().isTableEnabled(actualTable)) {
					store.getAdmin().disableTable(actualTable);
				}
				store.getAdmin().deleteTable(actualTable);
			}
		} catch (Exception x) {
			throw new RuntimeException(x);
		}
	}
	
	public HTableDescriptor getTableDescriptor() {
		return this.getTableDescriptor("");
	}
	
	public HTableDescriptor getTableDescriptor(String postfix) {
		try {
			return store.getAdmin().getTableDescriptor(Bytes.toBytes(table+postfix));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void createTable(boolean createDefault, boolean defaultInMem, Algorithm defaultCompr, boolean createOvr, boolean ovrInMem, Algorithm ovrCompr, boolean deferredLogFlush) {
		try {
			this.deleteTable();
			
			HTableDescriptor td = new HTableDescriptor(table);
			td.setDeferredLogFlush(deferredLogFlush);
			if (createDefault) {
				HColumnDescriptor cd = new HColumnDescriptor(defaultCfField.getName());
				cd.setInMemory(defaultInMem);
				cd.setCompressionType(defaultCompr);
				td.addFamily(cd);
			}
			if (createOvr) {
				HColumnDescriptor cd = new HColumnDescriptor(overloadedCfField.getName());
				cd.setInMemory(ovrInMem);
				cd.setCompressionType(ovrCompr);
				td.addFamily(cd);
			}
			
			store.getAdmin().createTable(td);
		} catch (Exception x) {
			throw new RuntimeException(x);
		}
	}
	
	public HColumnDescriptor getColumnFamilyDescriptor(Field field, HTableDescriptor td) {
		if (td == null)
			td = this.getTableDescriptor();
		return td.getFamily(Bytes.toBytes(field == null ? PropertyManagement.PROPERTY_COLUMNFAMILY_NAME : field.getName()));
	}
	
	public Map<String, Field> getChangedFields(boolean defaultCf, boolean overlodedCf, boolean dummyCf) {
		TreeMap<String, Field> ret = new TreeMap<String, Field>();
		if (defaultCf)
			ret.put(defaultCfField.getName(), defaultCfField);
		if (overlodedCf)
			ret.put(overloadedCfField.getName(), overloadedCfField);
		if (dummyCf)
			ret.put(dummyCfField.getName(), dummyCfField);
		return ret;
	}
	
	public ColumnFamilyData getChangedValues(boolean defaultCf, boolean overlodedCf, boolean dummyCf) {
		ColumnFamilyData ret = new DefaultColumnFamilyData();
		if (defaultCf) {
			Map<String, byte[]> change = new TreeMap<String, byte[]>();
			ret.put(defaultCfField.getName(), change);
			change.put(dummyQualifier, dummyValue);
		}
		if (overlodedCf) {
			Map<String, byte[]> change = new TreeMap<String, byte[]>();
			ret.put(overloadedCfField.getName(), change);
			change.put(dummyQualifier, dummyValue);
		}
		if (dummyCf) {
			Map<String, byte[]> change = new TreeMap<String, byte[]>();
			ret.put(dummyCfField.getName(), change);
			change.put(dummyQualifier, dummyValue);
		}
		return ret;
	}
	
	@Before
	public void restartStore() {
		store.restart();
	}

	@Test
	public void firstCreation() {
		this.deleteTable();
		store.storeChanges(new MetaInformation().forElement(elt).withColumnFamilies(this.getChangedFields(true, true, true)).withPostfixedTable(table, ""), table, id, this.getChangedValues(true, true, true), null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		HColumnDescriptor def = this.getColumnFamilyDescriptor(defaultCfField, td);
		HColumnDescriptor ovr = this.getColumnFamilyDescriptor(overloadedCfField, td);
		HColumnDescriptor dum = this.getColumnFamilyDescriptor(dummyCfField, td);
		
		assertFalse(td.isDeferredLogFlush());

		assertEquals(Algorithm.GZ, def.getCompression());
		assertTrue(def.isInMemory());
		assertEquals(Algorithm.GZ, ovr.getCompression());
		assertFalse(ovr.isInMemory());

		assertEquals(HColumnDescriptor.DEFAULT_BLOOMFILTER, def.getBloomFilterType().name());
		assertEquals(HColumnDescriptor.DEFAULT_BLOOMFILTER, dum.getBloomFilterType().name());
		assertEquals(StoreFile.BloomType.ROW, ovr.getBloomFilterType());

		assertEquals(HColumnDescriptor.DEFAULT_BLOCKCACHE, def.isBlockCacheEnabled());
		assertFalse(ovr.isBlockCacheEnabled());
		
		assertTrue(HColumnDescriptor.DEFAULT_BLOCKSIZE != 1234);
		assertEquals(HColumnDescriptor.DEFAULT_BLOCKSIZE, def.getBlocksize());
		assertEquals(HColumnDescriptor.DEFAULT_BLOCKSIZE, dum.getBlocksize());
		assertEquals(1234, ovr.getBlocksize());
		
		assertTrue(HColumnDescriptor.DEFAULT_VERSIONS != 1);
		assertEquals(HColumnDescriptor.DEFAULT_VERSIONS, def.getMaxVersions());
		assertEquals(HColumnDescriptor.DEFAULT_VERSIONS, dum.getMaxVersions());
		assertEquals(1, ovr.getMaxVersions());
		
		assertTrue(HColumnDescriptor.DEFAULT_REPLICATION_SCOPE != 1);
		assertEquals(HColumnDescriptor.DEFAULT_REPLICATION_SCOPE, def.getScope());
		assertEquals(HColumnDescriptor.DEFAULT_REPLICATION_SCOPE, dum.getScope());
		assertEquals(1, ovr.getScope());
		
		assertTrue(HColumnDescriptor.DEFAULT_TTL != 4321);
		assertEquals(HColumnDescriptor.DEFAULT_TTL, def.getTimeToLive());
		assertEquals(HColumnDescriptor.DEFAULT_TTL, dum.getTimeToLive());
		assertEquals(4321, ovr.getTimeToLive());
	}

	@Test
	public void alreadyCompressionNoneAllChanges() {
		this.createTable(true, false, Algorithm.NONE, true, false, Algorithm.NONE, false);
		store.storeChanges(new MetaInformation().forElement(elt).withColumnFamilies(this.getChangedFields(true, true, false)).withPostfixedTable(table, ""), table, id, this.getChangedValues(true, true, false), null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		HColumnDescriptor def = this.getColumnFamilyDescriptor(defaultCfField, td);
		HColumnDescriptor ovr = this.getColumnFamilyDescriptor(overloadedCfField, td);

		assertEquals(Algorithm.GZ, def.getCompression()); //inMemory was changed thus compression will also be changed
		assertTrue(def.isInMemory());
		assertEquals(Algorithm.GZ, ovr.getCompression());
		assertFalse(ovr.isInMemory());
	}

	@Test
	public void alreadyCompressionOvrOnlyChanges() {
		this.createTable(true, true, Algorithm.NONE, true, false, Algorithm.NONE, false);
		store.storeChanges(new MetaInformation().forElement(elt).withColumnFamilies(this.getChangedFields(true, true, false)).withPostfixedTable(table, ""), table, id, this.getChangedValues(true, true, false), null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		HColumnDescriptor def = this.getColumnFamilyDescriptor(defaultCfField, td);
		HColumnDescriptor ovr = this.getColumnFamilyDescriptor(overloadedCfField, td);

		assertEquals(Algorithm.NONE, def.getCompression());
		assertTrue(def.isInMemory()); //Was already the case ; default should not have changed at all
		assertEquals(Algorithm.GZ, ovr.getCompression());
		assertFalse(ovr.isInMemory());
	}
	
	@Test
	public void scanCaching() {
		Scan s = store.getScan(null, Element.class, null);
		assertEquals(1, s.getCaching());
	}
	
	@Test
	public void scanCachingSimpleFam() {
		Scan s = store.getScan(null, Element.class, this.getChangedFields(true, false, false));
		assertEquals(1, s.getCaching());
	}
	
	@Test
	public void scanCachingOverFam() {
		Scan s = store.getScan(null, Element.class, this.getChangedFields(true, true, false));
		assertEquals(2, s.getCaching());
	}
	
	@Test
	public void scanCachingOver2Fam() {
		Scan s = store.getScan(null, Element.class, this.getChangedFields(true, true, true));
		assertEquals(2, s.getCaching());
	}
	
	@Test
	public void scanCachingOverFam3Only() {
		Scan s = store.getScan(null, Element.class, this.getChangedFields(false, false, true));
		assertEquals(3, s.getCaching());
	}
	
	@Test
	public void overrideCFSchemaWithPostfix() {
		String postfix = "p1";
		try {
			this.deleteTable(postfix);
			DefaultHBaseSchema schema = new DefaultHBaseSchema();
			schema.setTimeToLiveInSeconds(998877);
			PropertyUtils.registerSchemaSpecificity(defaultCfField, postfix, schema);
			elt.postfix = postfix;
			store.storeChanges(new MetaInformation().forElement(elt).withColumnFamilies(this.getChangedFields(true, true, true)).withPostfixedTable(table, postfix), table+postfix, id, this.getChangedValues(true, true, true), null, null);
			
			HTableDescriptor td = this.getTableDescriptor(postfix);
			HColumnDescriptor def = this.getColumnFamilyDescriptor(defaultCfField, td);
			assertEquals(Algorithm.GZ, def.getCompression()); //Not overridden
			assertEquals(998877, def.getTimeToLive()); //Overridden
		} finally {
			PropertyUtils.clearAllSchemaSpecificities();
			elt.postfix="";
		}
	}
	
	@Test
	public void overrideCFSchemaWithEmptyPostfix() {
		try {
			this.deleteTable();
			DefaultHBaseSchema schema = new DefaultHBaseSchema();
			schema.setTimeToLiveInSeconds(998877);
			PropertyUtils.registerSchemaSpecificity(defaultCfField, "", schema);
			store.storeChanges(new MetaInformation().forElement(elt).withColumnFamilies(this.getChangedFields(true, true, true)).withPostfixedTable(table, ""), table, id, this.getChangedValues(true, true, true), null, null);
			
			HTableDescriptor td = this.getTableDescriptor();
			HColumnDescriptor def = this.getColumnFamilyDescriptor(defaultCfField, td);
			assertEquals(Algorithm.GZ, def.getCompression()); //Not overridden
			assertEquals(998877, def.getTimeToLive()); //Overridden
		} finally {
			PropertyUtils.clearAllSchemaSpecificities();
			elt.postfix="";
		}
	}
	
	@Test
	public void overrideCFSchemaWithBadPostfix() {
		String postfix = "p1";
		try {
			this.deleteTable(postfix);
			DefaultHBaseSchema schema = new DefaultHBaseSchema();
			schema.setTimeToLiveInSeconds(998877);
			PropertyUtils.registerSchemaSpecificity(defaultCfField, "toto", schema);
			elt.postfix = postfix;
			store.storeChanges(new MetaInformation().forElement(elt).withColumnFamilies(this.getChangedFields(true, true, true)).withPostfixedTable(table, postfix), table+postfix, id, this.getChangedValues(true, true, true), null, null);
			
			HTableDescriptor td = this.getTableDescriptor(postfix);
			HColumnDescriptor def = this.getColumnFamilyDescriptor(defaultCfField, td);
			assertEquals(Algorithm.GZ, def.getCompression()); //Not overridden
			assertEquals(HColumnDescriptor.DEFAULT_TTL, def.getTimeToLive()); //Overridden
		} finally {
			PropertyUtils.clearAllSchemaSpecificities();
			elt.postfix="";
		}
	}
	
	@Test
	public void overrideOverriddenCFSchemaWithPostfix() {
		String postfix = "p1";
		try {
			this.deleteTable(postfix);
			DefaultHBaseSchema schema = new DefaultHBaseSchema();
			schema.setTimeToLiveInSeconds(998877);
			PropertyUtils.registerSchemaSpecificity(overloadedCfField, postfix, schema);
			elt.postfix = postfix;
			store.storeChanges(new MetaInformation().forElement(elt).withColumnFamilies(this.getChangedFields(true, true, true)).withPostfixedTable(table, postfix), table+postfix, id, this.getChangedValues(true, true, true), null, null);
			
			HTableDescriptor td = this.getTableDescriptor(postfix);
			HColumnDescriptor ovr = this.getColumnFamilyDescriptor(overloadedCfField, td);
			assertEquals(Algorithm.GZ, ovr.getCompression()); //Not overridden
			assertFalse(ovr.isInMemory()); //Overridden in annotation
			assertEquals(998877, ovr.getTimeToLive()); //Overridden in PropertyUtils (overrides annotation)
		} finally {
			PropertyUtils.clearAllSchemaSpecificities();
			elt.postfix="";
		}
	}
	
	@Test
	public void overrideTableSchemaWithPostfix() {
		String postfix = "p1";
		try {
			this.deleteTable(postfix);
			DefaultHBaseSchema schema = new DefaultHBaseSchema();
			schema.setTimeToLiveInSeconds(998877);
			PropertyUtils.registerSchemaSpecificity(Element.class, postfix, schema);
			elt.postfix = postfix;
			store.storeChanges(new MetaInformation().forElement(elt).withColumnFamilies(this.getChangedFields(true, true, true)).withPostfixedTable(table, postfix), table+postfix, id, this.getChangedValues(true, true, true), null, null);
			
			HTableDescriptor td = this.getTableDescriptor(postfix);
			HColumnDescriptor def = this.getColumnFamilyDescriptor(defaultCfField, td);
			assertEquals(Algorithm.GZ, def.getCompression()); //Not overridden
			assertEquals(998877, def.getTimeToLive()); //Overridden
		} finally {
			PropertyUtils.clearAllSchemaSpecificities();
			elt.postfix="";
		}
	}
	
	@Test
	public void overrideTableAndCfSchemaWithPostfix() {
		String postfix = "p1";
		try {
			this.deleteTable(postfix);
			DefaultHBaseSchema schema = new DefaultHBaseSchema();
			schema.setTimeToLiveInSeconds(778899);
			schema.setCompression("none");
			PropertyUtils.registerSchemaSpecificity(Element.class, postfix, schema);
			schema = new DefaultHBaseSchema();
			schema.setTimeToLiveInSeconds(998877);
			PropertyUtils.registerSchemaSpecificity(defaultCfField, postfix, schema);
			elt.postfix = postfix;
			store.storeChanges(new MetaInformation().forElement(elt).withColumnFamilies(this.getChangedFields(true, true, true)).withPostfixedTable(table, postfix), table+postfix, id, this.getChangedValues(true, true, true), null, null);
			
			HTableDescriptor td = this.getTableDescriptor(postfix);
			HColumnDescriptor def = this.getColumnFamilyDescriptor(defaultCfField, td);
			assertTrue(def.isInMemory()); //Not overridden
			assertEquals(Algorithm.NONE, def.getCompression()); //Table-level overridden
			assertEquals(998877, def.getTimeToLive()); //Field-level overridden
		} finally {
			PropertyUtils.clearAllSchemaSpecificities();
			elt.postfix="";
		}
	}
	
	@Persisting(table="PropertyOverloadTest")
	@HBaseSchema(deferredLogFlush=SettableBoolean.TRUE)
	public static class DLFElement extends DummyPersistingElement {
		/**
		 * 
		 */
		private static final long serialVersionUID = -6655422356430807115L;
		@Key public String key;
	}
	
	@Test
	public void deferredLogFlush() {
		this.deleteTable();
		store.storeChanges(new MetaInformation().forElement(new DLFElement()).withColumnFamilies(this.getChangedFields(true, true, true)), table, id, null, null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		
		assertTrue(td.isDeferredLogFlush());
	}
	
	@Test
	public void deferredLogFlushChangedNotForced() {
		this.deleteTable();
		this.createTable(false, false, null, false, false, null, false);
		store.storeChanges(new MetaInformation().forElement(new DLFElement()).withColumnFamilies(this.getChangedFields(true, true, true)), table, id, null, null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		
		assertFalse(td.isDeferredLogFlush());
	}
	
	@Persisting(table="PropertyOverloadTest")
	@HBaseSchema(deferredLogFlush=SettableBoolean.FALSE)
	public static class NDLFElement extends DummyPersistingElement {
		/**
		 * 
		 */
		private static final long serialVersionUID = -6655422356430807115L;
		@Key public String key;
	}
	
	@Test
	public void notDeferredLogFlush() {
		this.deleteTable();
		store.storeChanges(new MetaInformation().forElement(new NDLFElement()).withColumnFamilies(this.getChangedFields(true, true, true)), table, id, null, null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		
		assertFalse(td.isDeferredLogFlush());

	}
	
	@Test
	public void notDeferredLogFlushChangedNotForced() {
		this.deleteTable();
		this.createTable(false, false, null, false, false, null, true);
		store.storeChanges(new MetaInformation().forElement(new NDLFElement()).withColumnFamilies(this.getChangedFields(true, true, true)), table, id, null, null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		
		assertTrue(td.isDeferredLogFlush());
	}
	
	@Persisting(table="PropertyOverloadTest")
	@HBaseSchema(deferredLogFlush=SettableBoolean.TRUE, forceDeferredLogFlush=SettableBoolean.FALSE)
	public static class DLFNotForcedElement extends DummyPersistingElement {
		/**
		 * 
		 */
		private static final long serialVersionUID = -6655422356430807115L;
		@Key public String key;
	}
	
	@Test
	public void deferredLogFlushNotForced() {
		this.deleteTable();
		store.storeChanges(new MetaInformation().forElement(new DLFNotForcedElement()).withColumnFamilies(this.getChangedFields(true, true, true)), table, id, null, null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		
		assertTrue(td.isDeferredLogFlush());
	}
	
	@Test
	public void deferredLogFlushChangedNotForcedNotForced() {
		this.deleteTable();
		this.createTable(false, false, null, false, false, null, false);
		store.storeChanges(new MetaInformation().forElement(new DLFNotForcedElement()).withColumnFamilies(this.getChangedFields(true, true, true)), table, id, null, null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		
		assertFalse(td.isDeferredLogFlush());
	}
	
	@Persisting(table="PropertyOverloadTest")
	@HBaseSchema(deferredLogFlush=SettableBoolean.TRUE, forceDeferredLogFlush=SettableBoolean.TRUE)
	public static class DLFForcedElement extends DummyPersistingElement {
		/**
		 * 
		 */
		private static final long serialVersionUID = -6655422356430807115L;
		@Key public String key;
	}
	
	@Test
	public void deferredLogFlushForced() {
		this.deleteTable();
		store.storeChanges(new MetaInformation().forElement(new DLFForcedElement()).withColumnFamilies(this.getChangedFields(true, true, true)), table, id, null, null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		
		assertTrue(td.isDeferredLogFlush());
	}
	
	@Test
	public void deferredLogFlushChangedForcedForced() {
		this.deleteTable();
		this.createTable(false, false, null, false, false, null, false);
		store.storeChanges(new MetaInformation().forElement(new DLFForcedElement()).withColumnFamilies(this.getChangedFields(true, true, true)), table, id, null, null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		
		assertTrue(td.isDeferredLogFlush());
	}
	
	@Persisting(table="PropertyOverloadTest")
	@HBaseSchema(deferredLogFlush=SettableBoolean.FALSE, forceDeferredLogFlush=SettableBoolean.TRUE)
	public static class NDLFForcedElement extends DummyPersistingElement {
		/**
		 * 
		 */
		private static final long serialVersionUID = -6655422356430807115L;
		@Key public String key;
	}
	
	@Test
	public void ndeferredLogFlushForced() {
		this.deleteTable();
		store.storeChanges(new MetaInformation().forElement(new NDLFForcedElement()).withColumnFamilies(this.getChangedFields(true, true, true)), table, id, null, null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		
		assertFalse(td.isDeferredLogFlush());
	}
	
	@Test
	public void ndeferredLogFlushChangedForcedForced() {
		this.deleteTable();
		this.createTable(false, false, null, false, false, null, true);
		store.storeChanges(new MetaInformation().forElement(new NDLFForcedElement()).withColumnFamilies(this.getChangedFields(true, true, true)), table, id, null, null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		
		assertFalse(td.isDeferredLogFlush());
	}

}
