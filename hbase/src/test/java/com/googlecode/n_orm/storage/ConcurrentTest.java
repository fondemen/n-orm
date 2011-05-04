package com.googlecode.n_orm.storage;

 import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.HConnection;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.hbase.Store;

public class ConcurrentTest {
	private static Store store1, store2;

	@BeforeClass
	public static void gettingSimilarStores() {
		HBaseLauncher.prepareHBase();
		store1 = HBaseLauncher.hbaseStore;
		HBaseLauncher.hbaseStore = null;
		HBaseLauncher.hBaseServer = null;
		HBaseLauncher.hbaseMaxRetries++;
		HBaseLauncher.prepareHBase();
		store2 = HBaseLauncher.hbaseStore;
	}
	
	@Test
	public void storesAreDifferent() {
		//Just to know if the test setup is well written...
		assertNotNull(store1);
		assertNotNull(store2);
		assertNotSame(store1, store2);
	}
	
	private void deleteTable(String table) throws IOException {

		if (store1.getAdmin().tableExists(table)) {
			store1.getAdmin().disableTable(table);
			store1.getAdmin().deleteTable(table);
		}
	}
	
	private void truncateTable(String table) throws IOException {

		if (store1.getAdmin().tableExists(table)) {
			store1.truncate(table, null);
			assertEquals(0, store1.count(table, null));
		}
	}
	
	@Test
	public void gettingEmptyObjectAndGetItFromBothStores() throws IOException {
		this.deleteTable("t1");
		
		store1.storeChanges("t1", "idt1", null, null, null);
		assertTrue(store1.exists("t1", "idt1"));
		assertTrue(store2.exists("t1", "idt1"));
		
		this.deleteTable("t1");
	}
	
	@Test
	public void acceptingOutsideTableRemoval() throws IOException {
		this.deleteTable("t1");
		
		Map<String, Map<String, byte[]>> change1 = new TreeMap<String, Map<String,byte[]>>();
		TreeMap<String, byte[]> ch1 = new TreeMap<String, byte[]>();
		change1.put("cf1", ch1);
		ch1.put("k1", new byte[]{1, 2});
		store1.storeChanges("t1", "idt1", change1 , null, null);
		store2.storeChanges("t1", "idt1", change1 , null, null);
		assertTrue(store2.exists("t1", "idt1", "cf1"));
		
		this.deleteTable("t1");
		
		store1.storeChanges("t1", "idt1", change1 , null, null);
		assertTrue(store2.exists("t1", "idt1", "cf1"));

		this.deleteTable("t1");
	}
	
	@Test
	public void acceptingConnectionTimeout() throws IOException, SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		this.truncateTable("t1");
		
		Map<String, Map<String, byte[]>> change1 = new TreeMap<String, Map<String,byte[]>>();
		TreeMap<String, byte[]> ch1 = new TreeMap<String, byte[]>();
		change1.put("cf1", ch1);
		ch1.put("k1", new byte[]{1, 2});
		
		HConnection cm = store1.getAdmin().getConnection();
		Method closeM = cm.getClass().getDeclaredMethod("close", boolean.class);
		closeM.setAccessible(true);
		closeM.invoke(cm, true);
		
		store1.storeChanges("t1", "idt1", change1 , null, null);
		assertTrue(store2.exists("t1", "idt1", "cf1"));

		this.truncateTable("t1");
	}
}
