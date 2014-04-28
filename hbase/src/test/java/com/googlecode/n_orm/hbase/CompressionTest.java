package com.googlecode.n_orm.hbase;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PropertyManagement;
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
		store.storeChanges(null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.NONE, propFamD.getCompression());
	}
	
	@Test
	public void testNoCompressionDefined() throws IOException {
		store.setCompression("none");
		store.storeChanges(null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.NONE, propFamD.getCompression());
	}
	
	@Test
	public void testGzCompressionDefined() throws IOException {
		assertTrue(org.apache.hadoop.hbase.util.CompressionTest.testCompression("gz"));
		store.setCompression("gz");
		store.storeChanges(null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.GZ, propFamD.getCompression());
	}
	
	@Test
	public void testTestedGzCompressionOrNone() throws IOException {
		store.setCompression("tested_gz-or-none");
		store.storeChanges(null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		Algorithm cmp = propFamD.getCompression();
		assertTrue(cmp.equals(Algorithm.GZ)) ;
	}
	
	@Test
	public void testTestedLzoCompressionOrGz() throws IOException {
		store.setCompression("tested_lzo-or-gz");
		store.storeChanges(null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		Algorithm cmp = propFamD.getCompression();
		if (org.apache.hadoop.hbase.util.CompressionTest.testCompression("lzo"))
			assertTrue(cmp.equals(Algorithm.LZO)) ;
		else
			assertTrue(cmp.equals(Algorithm.GZ)) ;
	}
	
	@Test
	public void testTestedLz4CompressionOrGz() throws IOException {
		store.setCompression("tested_lz4 -or- none");
		store.storeChanges(null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		Algorithm cmp = propFamD.getCompression();
		if (org.apache.hadoop.hbase.util.CompressionTest.testCompression("lz4"))
			assertTrue(cmp.equals(Algorithm.LZ4)) ;
		else
			assertTrue(cmp.equals(Algorithm.NONE)) ;
	}
	
	@Test
	public void testTestedSnappyCompressionOrGz() throws IOException {
		store.setCompression("tested_snappy -or- none");
		store.storeChanges(null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		Algorithm cmp = propFamD.getCompression();
		if (org.apache.hadoop.hbase.util.CompressionTest.testCompression("snappy"))
			assertTrue(cmp.equals(Algorithm.SNAPPY)) ;
		else
			assertTrue(cmp.equals(Algorithm.NONE)) ;
	}
	
	@Test
	public void testGZCompressionOrNone() throws IOException {
		store.setCompression("gz-or-none");
		store.storeChanges(null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		Algorithm cmp = propFamD.getCompression();
		assertTrue(cmp.equals(Algorithm.GZ)) ;
	}
	
	@Test
	public void testNoneCompressionOrGz() throws IOException {
		store.setCompression("none-or-gz");
		store.storeChanges(null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		Algorithm cmp = propFamD.getCompression();
		assertTrue(cmp.equals(Algorithm.NONE)) ;
	}
	
	@Test
	public void testNoneThenGzCompressionDefinedNotForced() throws IOException {
		store.setCompression("none");
		store.storeChanges(null, testTable, "row", null, null, null);
		store.setCompression("gz");
		store.storeChanges(null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.NONE, propFamD.getCompression());
	}
	
	@Test
	public void testNoneThenGzCompressionDefinedForced() throws IOException {
		try {
			store.setForceCompression(true);
			store.setCompression("none");
			store.storeChanges(null, testTable, "row", null, null, null);
			store.setCompression("gz");
			store.storeChanges(null, testTable, "row", null, null, null);
			HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
			assertEquals(Algorithm.GZ, propFamD.getCompression());
		} finally {
			store.setForceCompression(false);
		}
	}
	
	@Test
	public void testRecoveryCompressionDefinedWithFirst() throws IOException {
		
		store.setCompression("gz-or-none");
		store.storeChanges(null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.GZ, propFamD.getCompression());
	}
	
	@Test
	public void testRecoveryCompressionDefinedWithSecond() throws IOException {
		
		store.setCompression("dummy-or-gz");
		store.storeChanges(null, testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.GZ, propFamD.getCompression());
	}
	
	@Test(expected=DatabaseNotReachedException.class)
	public void testDummyCompressor() throws IOException {
		store.setCompression("dummy");
	}
}
