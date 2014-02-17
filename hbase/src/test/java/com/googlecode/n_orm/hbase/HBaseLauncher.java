package com.googlecode.n_orm.hbase;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.StoreTestLauncher;
import com.googlecode.n_orm.hbase.Store;


public class HBaseLauncher extends StoreTestLauncher {
	private static Properties hbaseProperties = null;

	public static String hbaseHost;
	public static Integer hbasePort = HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;
	public static Integer hbaseMaxRetries = 3;

	public static HBaseTestingUtility hBaseServer = null;
	public static com.googlecode.n_orm.hbase.Store hbaseStore;
	
	static {
		try {
			hbaseHost = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			hbaseHost = "localhost";
		}
	}

	public static Properties prepareHBase() {
		Properties p = new Properties();
		p.setProperty(StoreSelector.STORE_DRIVERCLASS_PROPERTY, com.googlecode.n_orm.hbase.Store.class.getName());
		p.setProperty(StoreSelector.STORE_DRIVERCLASS_STATIC_ACCESSOR, "getStore");

		if (hbaseStore == null && hBaseServer == null) {
			hbaseStore = com.googlecode.n_orm.hbase.Store.getStore(hbaseHost, hbasePort, hbaseMaxRetries);
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
					
					hbaseStore = com.googlecode.n_orm.hbase.Store.getStore(hbaseHost, hbasePort, hbaseMaxRetries);
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
/*
	@Override
	public Properties prepare(Class<?> testClass) {
		if (hbaseProperties == null)
			hbaseProperties = prepareHBase();
		return  hbaseProperties;
	}*/
//	@AfterClass
//	public static void shutdownHBase() {
//		try {
//			hBaseServer.shutdownMiniCluster();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}

	@Override
	public Properties prepare(Class<?> arg0) {
		if (hbaseProperties == null)
			hbaseProperties = prepareHBase();
		return  hbaseProperties;
	}
}
