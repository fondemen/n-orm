package com.googlecode.n_orm.storage;

 import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

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
	
	@Test
	public void gettingEmptyObjectAndGetItFromBothStores() {
		store1.storeChanges("t1", "idt1", null, null, null);
		assertTrue(store1.exists("t1", "idt1"));
		assertTrue(store2.exists("t1", "idt1"));
	}
	
	@Test
	public void gettingCfCreatedFromAnotherStore() throws IOException {
		if (store1.getAdmin().tableExists("t1")) {
			store1.getAdmin().disableTable("t1");
			store1.getAdmin().deleteTable("t1");
		}
		
		Map<String, Map<String, byte[]>> change1 = new TreeMap<String, Map<String,byte[]>>();
		TreeMap<String, byte[]> ch1 = new TreeMap<String, byte[]>();
		change1.put("cf1", ch1);
		ch1.put("k1", new byte[]{1, 2});
		store1.storeChanges("t1", "idt1", change1 , null, null);
		store2.storeChanges("t1", "idt1", change1 , null, null);
		assertTrue(store2.exists("t1", "idt1", "cf1"));
		
		change1.remove("cf1");
		change1.put("cf2", ch1);
		store1.storeChanges("t1", "idt1", change1 , null, null);
		store2.storeChanges("t1", "idt1", change1 , null, null);
		assertTrue(store2.exists("t1", "idt1", "cf2"));

		store1.getAdmin().disableTable("t1");
		store1.getAdmin().deleteTable("t1");
	}
}
