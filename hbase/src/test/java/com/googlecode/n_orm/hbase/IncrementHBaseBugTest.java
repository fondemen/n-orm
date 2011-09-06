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
import org.junit.Test;

import com.googlecode.n_orm.conversion.ConversionTools;

public class IncrementHBaseBugTest {
	
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
		store.storeChanges(testTable, testKey, null, null, all_incrs );
	}

	@Test
	public void testNoDelete() {
		Map<String, Map<String, Number>> all_incrs = new TreeMap<String, Map<String,Number>>();
		Map<String, Number> incrs = new TreeMap<String, Number>();
		all_incrs.put(testIncrCF, incrs);
		incrs.put(testIncrC, 1);
		store.storeChanges(testTable, testKey, null, null, all_incrs );
		
		byte[] data = store.get(testTable, testKey, testIncrCF, testIncrC);
		assertNotNull(data);
		int read = ConversionTools.convert(int.class, data);
		assertEquals(1, read);
		
		store.storeChanges(testTable, testKey, null, null, all_incrs );
		
		data = store.get(testTable, testKey, testIncrCF, testIncrC);
		assertNotNull(data);
		read = ConversionTools.convert(int.class, data);
		assertEquals(2, read);
	}

	@Test
	public void testDelete() {
		Map<String, Map<String, Number>> all_incrs = new TreeMap<String, Map<String,Number>>();
		Map<String, Number> incrs = new TreeMap<String, Number>();
		all_incrs.put(testIncrCF, incrs);
		incrs.put(testIncrC, 1);
		store.storeChanges(testTable, testKey, null, null, all_incrs );
		
		byte[] data = store.get(testTable, testKey, testIncrCF, testIncrC);
		assertNotNull(data);
		int read = ConversionTools.convert(int.class, data);
		assertEquals(1, read);
		
		store.delete(testTable, testKey);
		
		store.storeChanges(testTable, testKey, null, null, all_incrs );
		
		data = store.get(testTable, testKey, testIncrCF, testIncrC);
		assertNotNull(data);
		read = ConversionTools.convert(int.class, data);
		assertEquals(1, read); //Naive implementation of delete would return 2
	}

	/**
	 * There is a bug in HBase that make incrementing properties being retrieved even if they were deleted...
	 * @throws Exception
	 */
	@Test
	public void testDeleteWithFlush() throws Exception {
		Map<String, Map<String, Number>> all_incrs = new TreeMap<String, Map<String,Number>>();
		Map<String, Number> incrs = new TreeMap<String, Number>();
		all_incrs.put(testIncrCF, incrs);
		incrs.put(testIncrC, 1);
		store.storeChanges(testTable, testKey, null, null, all_incrs );
		
		byte[] data = store.get(testTable, testKey, testIncrCF, testIncrC);
		assertNotNull(data);
		int read = ConversionTools.convert(int.class, data);
		assertEquals(1, read);
		
		store.getAdmin().flush(testTable);
		
		store.delete(testTable, testKey);
		
		store.storeChanges(testTable, testKey, null, null, all_incrs );
		
		data = store.get(testTable, testKey, testIncrCF, testIncrC);
		assertNotNull(data);
		read = ConversionTools.convert(int.class, data);
		assertEquals(1, read); //Naive implementation of delete would return 2
	}

}
