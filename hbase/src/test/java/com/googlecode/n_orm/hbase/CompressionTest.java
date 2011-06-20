package com.googlecode.n_orm.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.compress.Compressor;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.PropertyManagement;

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
		store.storeChanges(testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store
				.getAdmin()
				.getTableDescriptor(Bytes.toBytes(testTable))
				.getFamily(
						Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.NONE, propFamD.getCompression());
	}

	@Test
	public void testNoCompressionDefined() throws IOException {
		store.setCompression("none");
		store.storeChanges(testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store
				.getAdmin()
				.getTableDescriptor(Bytes.toBytes(testTable))
				.getFamily(
						Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.NONE, propFamD.getCompression());
	}

	@Test
	public void testGzCompressionDefined() throws IOException {
		assertTrue(testCompression("gz")); // no such method in HBase 0.20.6
		store.setCompression("gz");
		store.storeChanges(testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store
				.getAdmin()
				.getTableDescriptor(Bytes.toBytes(testTable))
				.getFamily(
						Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.GZ, propFamD.getCompression());
	}

	@Test
	public void testLzoCompressionDefined() throws IOException {
		if (!testCompression("lzo")) {
			System.err.println("TEST WARNING: no LZO compression enabled");
			return;
		}

		store.setCompression("lzo");
		store.storeChanges(testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store
				.getAdmin()
				.getTableDescriptor(Bytes.toBytes(testTable))
				.getFamily(
						Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.LZO, propFamD.getCompression());
	}

	public static boolean testCompression(String codec) {
		return Store.testCompression(codec);
	}
	
	@Test
	public void testRecoveryCompressionDefinedWithFirst() throws IOException {
		
		store.setCompression("gz-or-none");
		store.storeChanges(testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.GZ, propFamD.getCompression());
	}
	
	@Test
	public void testRecoveryCompressionDefinedWithSecond() throws IOException {
		
		store.setCompression("dummy-or-gz");
		store.storeChanges(testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.GZ, propFamD.getCompression());
	}
}
