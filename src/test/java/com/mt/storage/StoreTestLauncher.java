package com.mt.storage;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import static org.junit.Assert.*;

import com.mt.storage.memory.Memory;

public class StoreTestLauncher {
	private static final Collection<Object[]> testedStores;
	
	static {
		testedStores = new ArrayList<Object[]>();
		Properties p;
		
		//Memory
		p = new Properties();
		p.setProperty(StoreSelector.STORE_DRIVERCLASS_PROPERTY, Memory.class.getName());
		p.setProperty(StoreSelector.STORE_DRIVERCLASS_SINGLETON_PROPERTY, "INSTANCE");
		testedStores.add(new Object[]{p});
		
		//HBase
		p = new Properties();
		p.setProperty(StoreSelector.STORE_DRIVERCLASS_PROPERTY, com.mt.storage.hbase.Store.class.getName());
		p.setProperty(StoreSelector.STORE_DRIVERCLASS_STATIC_ACCESSOR, "getStore");
		p.setProperty("1", "localhost");
		p.setProperty("2", "9000");
		testedStores.add(new Object[]{p});
		
		//Starting HBase server if necessary
		com.mt.storage.hbase.Store hbaseStore = com.mt.storage.hbase.Store.getStore(p.getProperty("1"), Integer.valueOf(p.getProperty("2")));
		try {
			hbaseStore.start();
			if (! hbaseStore.isStarted()) {
				new HBaseTestingUtility().startMiniCluster(1);
			}
		} catch (Exception e) {
			fail(e.getMessage());
		}
		assertTrue("Could not start HBase test server.", hbaseStore.isStarted());
	}
	
	public static Collection<Object[]> getTestedStores() {
		return new ArrayList<Object[]>(testedStores);
	}
	
	@SuppressWarnings("unchecked")
	public static void registerStorePropertiesForInnerClasses(Class<?> clazz, Properties props) {
		for (Class<?> c : clazz.getDeclaredClasses()) {
			if (PersistingElement.class.isAssignableFrom(c))
				StoreSelector.aspectOf().setPropertiesFor((Class<? extends PersistingElement>) c, props);
		}
	}

	private boolean isMemory;
	
	protected StoreTestLauncher(Properties props) {
		StoreTestLauncher.registerStorePropertiesForInnerClasses(this.getClass(), props);
		this.isMemory = props.get(StoreSelector.STORE_DRIVERCLASS_PROPERTY).equals(Memory.class.getName());
	}
	
	public void assertHadAQuery() {
		if (this.isMemory)
			assertTrue(Memory.INSTANCE.hadAQuery());
	}
	
	public void assertHadNoQuery() {
		if (this.isMemory)
			assertTrue(Memory.INSTANCE.hadNoQuery());
	}
	
	public void resetQueryCount() {
		if (this.isMemory)
			Memory.INSTANCE.resetQueries();
	}
}
