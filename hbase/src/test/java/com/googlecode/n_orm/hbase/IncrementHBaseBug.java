package com.googlecode.n_orm.hbase;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.googlecode.n_orm.BookStore;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.SimpleStorageTest;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.StoreTestLauncher;
import com.googlecode.n_orm.conversion.ConversionTools;

public class IncrementHBaseBug {
	
	private static Store store;
	private static String testTable = "incrtest";
	private static String testKey;
	private static String testIncrCF = "icf";
	private static String testIncrC = "ic";
	private static HBaseTestingUtility hBaseServer;

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
		store.storeChanges(null, testTable, testKey, null, null, all_incrs );
	}

	@Test
	public void testNodelete() {
		Map<String, Map<String, Number>> all_incrs = new TreeMap<String, Map<String,Number>>();
		Map<String, Number> incrs = new TreeMap<String, Number>();
		all_incrs.put(testIncrCF, incrs);
		incrs.put(testIncrC, 1);
		store.storeChanges(null, testTable, testKey, null, null, all_incrs );
		
		byte[] data = store.get(null, testTable, testKey, testIncrCF, testIncrC);
		assertNotNull(data);
		int read = ConversionTools.convert(int.class, data);
		assertEquals(1, read);
		
		store.storeChanges(null, testTable, testKey, null, null, all_incrs );
		
		data = store.get(null, testTable, testKey, testIncrCF, testIncrC);
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
		store.storeChanges(null, testTable, testKey, null, null, all_incrs );
		
		byte[] data = store.get(null, testTable, testKey, testIncrCF, testIncrC);
		assertNotNull(data);
		int read = ConversionTools.convert(int.class, data);
		assertEquals(1, read);
		
		store.delete(null, testTable, testKey);
		
		store.storeChanges(null, testTable, testKey, null, null, all_incrs );
		
		data = store.get(null, testTable, testKey, testIncrCF, testIncrC);
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
		store.storeChanges(null, testTable, testKey, null, null, all_incrs );
		
		byte[] data; int read;
//		data = store.get(null, null, testTable, testKey, testIncrCF, testIncrC);
//		assertNotNull(data);
//		read = ConversionTools.convert(int.class, data);
//		assertEquals(1, read);
		
		store.getAdmin().flush(testTable);
		store.delete(null, testTable, testKey);
		
		store.storeChanges(null, testTable, testKey, null, null, all_incrs );
		
		data = store.get(null, testTable, testKey, testIncrCF, testIncrC);
		assertNotNull(data);
		read = ConversionTools.convert(int.class, data);
		assertEquals(1, read); //Naive implementation of delete would return 2
	}
	
	@Test
	public void testWithElement() {
		SimpleStorageTest.IncrementingElement elt = new SimpleStorageTest.IncrementingElement(this.testKey);
		elt.setStore(store);
		
		elt.ival++;
		elt.store();
		
		elt.delete();

		elt = new SimpleStorageTest.IncrementingElement(this.testKey);
		elt.setStore(store);
		
		elt.ival++;
		elt.store();

		elt = new SimpleStorageTest.IncrementingElement(this.testKey);
		elt.setStore(store);
		elt.activate();
		assertEquals(1, elt.ival);
	}
	
	@Ignore
	@Test
	public void testWithElementWithFlush() throws IOException, InterruptedException {
		SimpleStorageTest.IncrementingElement elt = new SimpleStorageTest.IncrementingElement(this.testKey);
		elt.setStore(store);
		
		elt.ival++;
		elt.store();

		store.getAdmin().flush(elt.getTable());
		elt.delete();

		elt = new SimpleStorageTest.IncrementingElement(this.testKey);
		elt.setStore(store);
		
		elt.ival++;
		elt.store();

		elt = new SimpleStorageTest.IncrementingElement(this.testKey);
		elt.setStore(store);
		elt.activate();
		assertEquals(1, elt.ival);
	}
	
	@Test
	public void testWithElementWithRestart() throws IOException, InterruptedException {
		this.testKey = "testflush";
		SimpleStorageTest.IncrementingElement elt = new SimpleStorageTest.IncrementingElement(this.testKey);
		elt.setStore(store);
		
		elt.ival++;
		elt.store();

		elt = new SimpleStorageTest.IncrementingElement(this.testKey);
		elt.setStore(store);
		elt.activate();
		assertEquals(1, elt.ival);

		elt = new SimpleStorageTest.IncrementingElement(this.testKey);
		elt.setStore(store);
		elt.delete();
	}

}
