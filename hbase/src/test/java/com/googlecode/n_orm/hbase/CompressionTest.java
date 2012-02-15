package com.googlecode.n_orm.hbase;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.googlecode.n_orm.Book;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.hbase.Store;


public class CompressionTest {
	
	
	private static Store store;
	private static String testTable = "compressiontest";
	private static String defaultCompression;

	@BeforeClass
	public static void prepareStore() {
		HBaseLauncher.prepareHBase();
		store = HBaseLauncher.hbaseStore;
		defaultCompression = store.getCompression();
	}
	
	@Before
	@After
	public void deleteIfExists() throws IOException {
		if (store.getAdmin().tableExists(testTable)) {
			store.getAdmin().disableTable(testTable);
			store.getAdmin().deleteTable(testTable);
		}
		store.setCompression(defaultCompression);
	}
	
	@Test
	public void testNoCompressionNullDefined() throws IOException {
		store.setCompression(null);
		store.storeChanges(null, null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.NONE, propFamD.getCompression());
	}
	
	@Test
	public void testNoCompressionDefined() throws IOException {
		store.setCompression("none");
		store.storeChanges(null, null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.NONE, propFamD.getCompression());
	}
	
	@Test
	public void testGzCompressionDefined() throws IOException {
		assertTrue(org.apache.hadoop.hbase.util.CompressionTest.testCompression("gz"));
		store.setCompression("gz");
		store.storeChanges(null, null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.GZ, propFamD.getCompression());
	}
	
	@Test
	@Ignore //No way to test LZO compression on an HBase cluster ; including that one for tests
	public void testLzoCompressionDefined() throws IOException {
		store.setCompression("lzo");
		store.storeChanges(null, null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		Algorithm cmp = propFamD.getCompression();
		assertTrue(cmp.equals(Algorithm.LZO) || cmp.equals(Algorithm.NONE)) ;
	}
	
	@Test
	public void testNoneThenGzCompressionDefinedNotForced() throws IOException {
		store.setCompression("none");
		store.storeChanges(null, null, testTable, "row", null, null, null);
		store.setCompression("gz");
		store.storeChanges(null, null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.NONE, propFamD.getCompression());
	}
	
	@Test
	public void testNoneThenGzCompressionDefinedForced() throws IOException {
		try {
			store.setForceCompression(true);
			store.setCompression("none");
			store.storeChanges(null, null, testTable, "row", null, null, null);
			store.setCompression("gz");
			store.storeChanges(null, null, testTable, "row", null, null, null);
			HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
			assertEquals(Algorithm.GZ, propFamD.getCompression());
		} finally {
			store.setForceCompression(false);
		}
	}
	
	@Test
	public void testRecoveryCompressionDefinedWithFirst() throws IOException {
		
		store.setCompression("gz-or-none");
		store.storeChanges(null, null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.GZ, propFamD.getCompression());
	}
	
	@Test
	public void testRecoveryCompressionDefinedWithSecond() throws IOException {
		
		store.setCompression("dummy-or-gz");
		store.storeChanges(null, null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.GZ, propFamD.getCompression());
	}
}
