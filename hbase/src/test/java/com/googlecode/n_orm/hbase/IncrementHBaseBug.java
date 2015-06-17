package com.googlecode.n_orm.hbase;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.googlecode.n_orm.SimpleStorageTest;
import com.googlecode.n_orm.conversion.ConversionTools;

public class IncrementHBaseBug {
	
	private static Store store;
	private static TableName testTable = TableName.valueOf("incrtest");
	private static String testKey;
	private static String testIncrCF = "icf";
	private static String testIncrC = "ic";

	@BeforeClass
	public static void startCluster() throws Exception {
		HBaseLauncher.prepareHBase();
		store = HBaseLauncher.hbaseStore;
	}
	
	@Before
	public void setNewKey() {
		testKey = UUID.randomUUID().toString();
	}

	@Test(expected=Test.None.class)
	public void testNoIncr() {
		Map<String, Map<String, Number>> all_incrs = new TreeMap<String, Map<String,Number>>();
		Map<String, Number> incrs = new TreeMap<String, Number>();
		all_incrs.put(testIncrCF, incrs);
		store.storeChanges(null, testTable.getNameAsString(), testKey, null, null, all_incrs );
	}

	@Test
	public void testNodelete() {
		Map<String, Map<String, Number>> all_incrs = new TreeMap<String, Map<String,Number>>();
		Map<String, Number> incrs = new TreeMap<String, Number>();
		all_incrs.put(testIncrCF, incrs);
		incrs.put(testIncrC, 1);
		store.storeChanges(null, testTable.getNameAsString(), testKey, null, null, all_incrs );
		
		byte[] data = store.get(null, testTable.getNameAsString(), testKey, testIncrCF, testIncrC);
		assertNotNull(data);
		int read = ConversionTools.convert(int.class, data);
		assertEquals(1, read);
		
		store.storeChanges(null, testTable.getNameAsString(), testKey, null, null, all_incrs );
		
		data = store.get(null, testTable.getNameAsString(), testKey, testIncrCF, testIncrC);
		assertNotNull(data);
		read = ConversionTools.convert(int.class, data);
		assertEquals(2, read);
	}

	@Test
	@Ignore
	public void testdelete() {
		Map<String, Map<String, Number>> all_incrs = new TreeMap<String, Map<String,Number>>();
		Map<String, Number> incrs = new TreeMap<String, Number>();
		all_incrs.put(testIncrCF, incrs);
		incrs.put(testIncrC, 1);
		store.storeChanges(null, testTable.getNameAsString(), testKey, null, null, all_incrs );
		
		byte[] data = store.get(null, testTable.getNameAsString(), testKey, testIncrCF, testIncrC);
		assertNotNull(data);
		int read = ConversionTools.convert(int.class, data);
		assertEquals(1, read);
		
		store.delete(null, testTable.getNameAsString(), testKey);
		
		store.storeChanges(null, testTable.getNameAsString(), testKey, null, null, all_incrs );
		
		data = store.get(null, testTable.getNameAsString(), testKey, testIncrCF, testIncrC);
		assertNotNull(data);
		read = ConversionTools.convert(int.class, data);
		assertEquals(1, read); //Naive implementation of delete would return 2
	}

	/**
	 * There is a bug in HBase that make incrementing properties being retrieved even if they were deleted...
	 * @throws Exception
	 */
	@Ignore
	@Test
	public void testDeleteWithFlush() throws Exception {
		Map<String, Map<String, Number>> all_incrs = new TreeMap<String, Map<String,Number>>();
		Map<String, Number> incrs = new TreeMap<String, Number>();
		all_incrs.put(testIncrCF, incrs);
		incrs.put(testIncrC, 1);
		store.storeChanges(null, testTable.getNameAsString(), testKey, null, null, all_incrs );
		
		byte[] data; int read;
//		data = store.get(null, null, testTable, testKey, testIncrCF, testIncrC);
//		assertNotNull(data);
//		read = ConversionTools.convert(int.class, data);
//		assertEquals(1, read);
		
		Admin admin = store.getConnection().getAdmin();
		try {
			admin.flush(testTable);
		} finally {
			admin.close();
		}
		
		store.delete(null, testTable.getNameAsString(), testKey);
		
		store.storeChanges(null, testTable.getNameAsString(), testKey, null, null, all_incrs );
		
		data = store.get(null, testTable.getNameAsString(), testKey, testIncrCF, testIncrC);
		assertNotNull(data);
		read = ConversionTools.convert(int.class, data);
		assertEquals(1, read); //Naive implementation of delete would return 2
	}
	
	@Test
	public void testWithElement() {
		SimpleStorageTest.IncrementingElement elt = new SimpleStorageTest.IncrementingElement(testKey);
		elt.setStore(store);
		
		elt.ival++;
		elt.store();
		
		elt.delete();

		elt = new SimpleStorageTest.IncrementingElement(testKey);
		elt.setStore(store);
		
		elt.ival++;
		elt.store();

		elt = new SimpleStorageTest.IncrementingElement(testKey);
		elt.setStore(store);
		elt.activate();
		assertEquals(1, elt.ival);
	}
	
	@Ignore
	@Test
	public void testWithElementWithFlush() throws IOException, InterruptedException {
		SimpleStorageTest.IncrementingElement elt = new SimpleStorageTest.IncrementingElement(testKey);
		elt.setStore(store);
		
		elt.ival++;
		elt.store();

		HBaseAdmin admin = new HBaseAdmin(store.getConnection());
		try {
			admin.flush(elt.getTable());
		} finally {
			admin.close();
		}
		elt.delete();

		elt = new SimpleStorageTest.IncrementingElement(testKey);
		elt.setStore(store);
		
		elt.ival++;
		elt.store();

		elt = new SimpleStorageTest.IncrementingElement(testKey);
		elt.setStore(store);
		elt.activate();
		assertEquals(1, elt.ival);
	}
	
	@Test
	public void testWithElementWithRestart() throws IOException, InterruptedException {
		testKey = "testflush";
		SimpleStorageTest.IncrementingElement elt = new SimpleStorageTest.IncrementingElement(testKey);
		elt.setStore(store);
		
		elt.ival++;
		elt.store();

		elt = new SimpleStorageTest.IncrementingElement(testKey);
		elt.setStore(store);
		elt.activate();
		assertEquals(1, elt.ival);

		elt = new SimpleStorageTest.IncrementingElement(testKey);
		elt.setStore(store);
		elt.delete();
	}

}
