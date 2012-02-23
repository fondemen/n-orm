package com.googlecode.n_orm.hbase;

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.cf.SetColumnFamily;
import com.googlecode.n_orm.hbase.HBaseSchema.SettableBoolean;

public class PropertyOverloadTest {
	private static Store store;
	private static String table = "PropertyOverloadTest";
	private static String key = "YT78I6YJ890NF6B_ht-èit-";
	private static final String id = key + KeyManagement.KEY_SEPARATOR;
	private static String dummyQualifier = "78YGI8G67H90";
	private static byte[] dummyValue = Bytes.toBytes("787RC56Y0J9YYJ");
	private static final Element elt = new Element();
	private static final Field defaultCfField, overloadedCfField, dummyCfField;
	
	static {
		try {
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
	
	//Just a dummy class so that we've got something to give as parameter...
	@Persisting
	@HBaseSchema(compression="gz", forceInMemory=SettableBoolean.TRUE, inMemory=SettableBoolean.TRUE)
	public static class Element extends DummyPersistingElement {
		private static final long serialVersionUID = 1L;

		@Key public String key = PropertyOverloadTest.key;
		
		public SetColumnFamily<String> defaultCf = new SetColumnFamily<String>();
		@HBaseSchema(forceCompression=SettableBoolean.TRUE, inMemory=SettableBoolean.FALSE, bloomFilterType="ROW", blockCacheEnabled=SettableBoolean.FALSE, blockSize=1234, maxVersions=1, replicationScope=1, timeToLiveInSeconds=4321) public SetColumnFamily<String> overlodedCf = new SetColumnFamily<String>();
		@HBaseSchema(bloomFilterType="dummy", blockSize=-34, maxVersions=-12, replicationScope=2, timeToLiveInSeconds=-13) public SetColumnFamily<String> dummyCf = new SetColumnFamily<String>();
	}
	
	public void deleteTable() {
		try {
			if (store.getAdmin().tableExists(table)) {
				if (store.getAdmin().isTableEnabled(table)) {
					store.getAdmin().disableTable(table);
				}
				store.getAdmin().deleteTable(table);
			}
		} catch (Exception x) {
			throw new RuntimeException(x);
		}
	}
	
	public HTableDescriptor getTableDescriptor() {
		try {
			return store.getAdmin().getTableDescriptor(Bytes.toBytes(table));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void createTable(boolean createDefault, boolean defaultInMem, Algorithm defaultCompr, boolean createOvr, boolean ovrInMem, Algorithm ovrCompr) {
		try {
			this.deleteTable();
			
			HTableDescriptor td = new HTableDescriptor(table);
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
	
	public Map<String, Map<String, byte[]>> getChangedValues(boolean defaultCf, boolean overlodedCf, boolean dummyCf) {
		Map<String, Map<String, byte[]>> ret = new TreeMap<String, Map<String, byte[]>>();
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
		store.storeChanges(elt, this.getChangedFields(true, true, true), table, id, this.getChangedValues(true, true, true), null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		HColumnDescriptor def = this.getColumnFamilyDescriptor(defaultCfField, td);
		HColumnDescriptor ovr = this.getColumnFamilyDescriptor(overloadedCfField, td);
		HColumnDescriptor dum = this.getColumnFamilyDescriptor(dummyCfField, td);

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
		this.createTable(true, false, Algorithm.NONE, true, false, Algorithm.NONE);
		store.storeChanges(elt, this.getChangedFields(true, true, false), table, id, this.getChangedValues(true, true, false), null, null);
		
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
		this.createTable(true, true, Algorithm.NONE, true, false, Algorithm.NONE);
		store.storeChanges(elt, this.getChangedFields(true, true, false), table, id, this.getChangedValues(true, true, false), null, null);
		
		HTableDescriptor td = this.getTableDescriptor();
		HColumnDescriptor def = this.getColumnFamilyDescriptor(defaultCfField, td);
		HColumnDescriptor ovr = this.getColumnFamilyDescriptor(overloadedCfField, td);

		assertEquals(Algorithm.NONE, def.getCompression());
		assertTrue(def.isInMemory()); //Was already the case ; default should not have changed at all
		assertEquals(Algorithm.GZ, ovr.getCompression());
		assertFalse(ovr.isInMemory());
	}

}
