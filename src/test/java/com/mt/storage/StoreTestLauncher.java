package com.mt.storage;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.mt.storage.memory.Memory;

public class StoreTestLauncher {
	private static Collection<Object[]> testedStores = null;

	private static String hbaseUrl;
	private static int hbaseMaxRetries = 3;

	private static HBaseTestingUtility hBaseServer = null;

	public static void prepareStores() {
		testedStores = new ArrayList<Object[]>();

		testedStores.add(new Object[] { prepareMemory() });
		testedStores.add(new Object[] { prepareHBase() });

		
	}

	protected static Properties prepareMemory() {
		Properties p = new Properties();
		p.setProperty(StoreSelector.STORE_DRIVERCLASS_PROPERTY,
				Memory.class.getName());
		p.setProperty(StoreSelector.STORE_DRIVERCLASS_SINGLETON_PROPERTY,
				"INSTANCE");
		return p;
	}

	protected static Properties prepareHBase() {
		Properties p = new Properties();
		p.setProperty(StoreSelector.STORE_DRIVERCLASS_PROPERTY,
				com.mt.storage.hbase.Store.class.getName());
		p.setProperty(StoreSelector.STORE_DRIVERCLASS_STATIC_ACCESSOR,
				"getStore");

		
		if (hBaseServer == null)
		// Starting HBase server
		try {
			hBaseServer = new HBaseTestingUtility();
			hBaseServer.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
			hBaseServer.getConfiguration().setInt("hbase.client.pause", 250);
			hBaseServer.getConfiguration().setInt("hbase.client.retries.number", hbaseMaxRetries);
			//hBaseServer.getConfiguration().set(HConstants.HBASE_DIR, hbaseUrl);
			
			hBaseServer.startMiniCluster(1);
			hbaseUrl = hBaseServer.getConfiguration().get(HConstants.HBASE_DIR, hbaseUrl);
			p.setProperty("1", hbaseUrl);
			
			com.mt.storage.hbase.Store hStore = com.mt.storage.hbase.Store.getStore(hbaseUrl, hbaseMaxRetries);
			hStore.setConf(hBaseServer.getConfiguration());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		p.setProperty("1", hbaseUrl);
		p.setProperty("2", Integer.toString(hbaseMaxRetries));
			
		return p;
	}

//	@AfterClass
//	public static void shutdownHBase() {
//		try {
//			hBaseServer.shutdownMiniCluster();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}

	public static Collection<Object[]> getTestedStores() {
		prepareStores();
		return new ArrayList<Object[]>(testedStores);
	}

	@SuppressWarnings("unchecked")
	public static void registerStorePropertiesForInnerClasses(Class<?> clazz,
			Properties props) {
		for (Class<?> c : clazz.getDeclaredClasses()) {
			if (PersistingElement.class.isAssignableFrom(c))
				StoreSelector.aspectOf().setPropertiesFor(
						(Class<? extends PersistingElement>) c, props);
		}
	}

	private boolean isMemory;

	protected StoreTestLauncher(Properties props) {
		StoreTestLauncher.registerStorePropertiesForInnerClasses(
				this.getClass(), props);
		this.isMemory = props.get(StoreSelector.STORE_DRIVERCLASS_PROPERTY)
				.equals(Memory.class.getName());
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
