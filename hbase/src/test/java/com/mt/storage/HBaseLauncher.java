package com.mt.storage;

import java.util.Properties;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;

import com.mt.storage.hbase.Store;

public class HBaseLauncher extends StoreTestLauncher {
	private static Properties hbaseProperties = null;

	public static String hbaseHost = "localhost";
	public static Integer hbasePort = HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;
	public static Integer hbaseMaxRetries = 3;

	public static HBaseTestingUtility hBaseServer = null;
	public static com.mt.storage.hbase.Store hbaseStore;

	public static Properties prepareHBase() {
		Properties p = new Properties();
		p.setProperty(StoreSelector.STORE_DRIVERCLASS_PROPERTY, com.mt.storage.hbase.Store.class.getName());
		p.setProperty(StoreSelector.STORE_DRIVERCLASS_STATIC_ACCESSOR, "getStore");

		if (hbaseStore == null && hBaseServer == null) {
			hbaseStore = com.mt.storage.hbase.Store.getStore(hbaseHost, hbasePort, hbaseMaxRetries);
			try {
				hbaseStore.start();
			} catch (DatabaseNotReachedException x) {
				// Starting HBase server
				try {
					hBaseServer = new HBaseTestingUtility();
					hBaseServer.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
					hBaseServer.getConfiguration().setInt("hbase.client.pause", 250);
					hBaseServer.getConfiguration().setInt("hbase.client.retries.number", hbaseMaxRetries);
//					if (hbaseHost != null)
//						hBaseServer.getConfiguration().set(HConstants.ZOOKEEPER_QUORUM, hbaseHost);
//					if (hbasePort != null)
//						hBaseServer.getConfiguration().setInt("hbase.zookeeper.property.clientPort", hbasePort);
					
					hBaseServer.startMiniCluster(1);
					hbaseHost = hBaseServer.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM);
					hbasePort = hBaseServer.getConfiguration().getInt("hbase.zookeeper.property.clientPort", HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT);
					
					hbaseStore = com.mt.storage.hbase.Store.getStore(hbaseHost, hbasePort, hbaseMaxRetries);
					hbaseStore.setConf(hBaseServer.getConfiguration());
					hbaseStore.setAdmin(hBaseServer.getHBaseAdmin());
					hbaseStore.start();
					
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}

		p.setProperty("1", hbaseHost);
		p.setProperty("2", hbasePort.toString());
		p.setProperty("3", Integer.toString(hbaseMaxRetries));
			
		return p;
	}

	public static void setConf(Store store) {
		store.setConf(hBaseServer.getConfiguration());
	}

	@Override
	public Properties prepare(Class<?> testClass) {
		if (hbaseProperties == null)
			hbaseProperties = prepareHBase();
		return hbaseProperties;
	}

//	@AfterClass
//	public static void shutdownHBase() {
//		try {
//			hBaseServer.shutdownMiniCluster();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
}