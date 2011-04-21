package com.googlecode.n_orm.storage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.hbase.Store;

public class CountTest {
	private static Store store;
	private static final String testTable = "testtable";
	
	
	@BeforeClass
	public static void prepareStore() {
		HBaseLauncher.prepareHBase();
		store = HBaseLauncher.hbaseStore;
	}
	
	@Before
	public void deleteTestTable() throws IOException {

		if (store.getAdmin().tableExists(testTable)) {
			store.getAdmin().disableTable(testTable);
			store.getAdmin().deleteTable(testTable);
		}
	}

	@Test
	public void inexistingTable() {
		assertEquals(0l, store.count(testTable, null));
		
	}

	@Test
	public void oneEmpty() {
		store.storeChanges(testTable, "testid", null, null, null);
		assertEquals(1l, store.count(testTable, null));
		
	}

	@Test
	public void one() {
		Map<String, Map<String, byte[]>> change = new TreeMap<String, Map<String,byte[]>>();
		Map<String, byte[]> famChange = new TreeMap<String, byte[]>();
		famChange.put("col", new byte[]{1, 2, 3});
		change.put("fam", famChange );
		store.storeChanges(testTable, "testid", change , null, null);
		assertEquals(1l, store.count(testTable, null));
		
	}
	
	@Test
	public void two() {
		Map<String, Map<String, byte[]>> change = new TreeMap<String, Map<String,byte[]>>();
		Map<String, byte[]> famChange = new TreeMap<String, byte[]>();
		famChange.put("col", new byte[]{1, 2, 3});
		change.put("fam", famChange );
		store.storeChanges(testTable, "testid", change , null, null);
		store.storeChanges(testTable, "testid2", null , null, null);
		assertEquals(2l, store.count(testTable, null));
	}
}
