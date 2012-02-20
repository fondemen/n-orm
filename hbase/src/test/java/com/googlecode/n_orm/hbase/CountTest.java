package com.googlecode.n_orm.hbase;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.hbase.Store;
import com.googlecode.n_orm.storeapi.Constraint;

public class CountTest {
	private static Store store;
	private static final String testTable = "testtable";
	
	
	@BeforeClass
	public static void prepareStore() {
		HBaseLauncher.prepareHBase();
		store = HBaseLauncher.hbaseStore;
	}
	
	@Before
	@After
	public void truncateTestTable() throws IOException {

		if (store.getAdmin().tableExists(testTable)) {
			store.truncate(null, testTable, (Constraint)null);
			assertEquals(0, store.count(null, testTable, (Constraint)null));
		}
	}
	
	public void deleteTestTable() throws IOException {

		if (store.getAdmin().tableExists(testTable)) {
			store.getAdmin().disableTable(testTable);
			store.getAdmin().deleteTable(testTable);
		}
	}

	@Test
	public void inexistingTable() throws IOException {
		assertEquals(0l, store.count(null, "huidhzidxeozd", (Constraint)null));
		
	}

	@Test
	public void none() {
		Map<String, Map<String, byte[]>> change = new TreeMap<String, Map<String,byte[]>>();
		Map<String, byte[]> famChange = new TreeMap<String, byte[]>();
		famChange.put("col", new byte[]{1, 2, 3});
		change.put("fam", famChange );
		store.storeChanges(null, null, testTable, "testid", change , null, null);
		store.delete(null, testTable, "testid");
		assertEquals(0l, store.count(null, testTable, (Constraint)null));
		
	}

	@Test
	public void oneEmpty() {
		store.storeChanges(null, null, testTable, "testid", null, null, null);
		assertEquals(1l, store.count(null, testTable, (Constraint)null));
		
	}

	@Test
	public void one() {
		Map<String, Map<String, byte[]>> change = new TreeMap<String, Map<String,byte[]>>();
		Map<String, byte[]> famChange = new TreeMap<String, byte[]>();
		famChange.put("col1", new byte[]{1, 2, 3});
		famChange.put("col2", new byte[]{1, 2, 3});
		change.put("fam1", famChange );
		change.put("fam2", famChange );
		store.storeChanges(null, null, testTable, "testid", change , null, null);
		assertEquals(1l, store.count(null, testTable, (Constraint)null));
		
	}
	
	@Test
	public void two() {
		Map<String, Map<String, byte[]>> change = new TreeMap<String, Map<String,byte[]>>();
		Map<String, byte[]> famChange = new TreeMap<String, byte[]>();
		famChange.put("col", new byte[]{1, 2, 3});
		change.put("fam", famChange );
		store.storeChanges(null, null, testTable, "testid", change , null, null);
		store.storeChanges(null, null, testTable, "testid2", null , null, null);
		assertEquals(2l, store.count(null, testTable, (Constraint)null));
	}
	
	@Test
	public void hundred() {
		for(int i = 0 ; i < 100; ++i) {
			store.storeChanges(null, null, testTable, "testid"+i, null , null, null);
		}
		assertEquals(100l, store.count(null, testTable, (Constraint)null));
	}
	
	@Test
	public void hundredFrom33to66() {
		for(int i = 0 ; i < 100; ++i) {
			store.storeChanges(null, null, testTable, ConversionTools.convertToString(i), null , null, null);
		}
		assertEquals(1+66-33, store.count(null, testTable, new Constraint(ConversionTools.convertToString(33), ConversionTools.convertToString(66))));
	}
	
	@Test
	public void thousandsFrom33to66() {
		for(int i = 0 ; i < 10000; ++i) {
			store.storeChanges(null, null, testTable, ConversionTools.convertToString(i), null , null, null);
		}
		assertEquals(1+66-33, store.count(null, testTable, new Constraint(ConversionTools.convertToString(33), ConversionTools.convertToString(66))));
	}
}
