package com.googlecode.n_orm.hbase;

 import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.hbase.HBaseLauncher;
import com.googlecode.n_orm.hbase.Store;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;


public class ConcurrentTest {
	private static class PutElement implements Runnable {
		private final int[] done;
		private final Store store;
		private Throwable error;
		private volatile boolean go = false;

		private PutElement(int[] done, Store store) {
			this.done = done;
			this.store = store;
		}
		
		public void go() {
			go = true;
		}

		@Override
		public void run() {
			while (!go)
				try {
					Thread.sleep(10);
				} catch (InterruptedException e1) {
				}
			try {
				ColumnFamilyData ch = new DefaultColumnFamilyData();
				Map<String, byte[]> cf = new TreeMap<String, byte[]>();
				cf.put("qual", new byte [] {1, 2, 3, 4});
				ch.put("cf", cf );
				this.store.storeChanges(null, null, "t1", "idt1", ch , null, null);
			} catch (Throwable e) {
				this.error = e;
			} finally {
				done[0]--;
			}
		}
		
		public Throwable getError() {
			return this.error;
		}
	}

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
	
	private void disableTable(String table) throws IOException {

		if (store1.getAdmin().tableExists(table)) {
			store1.getAdmin().disableTable(table);
		}
	}
	
	private void truncateTable(String table) throws IOException {

		if (store1.getAdmin().tableExists(table)) {
			store1.truncate(null, table, (Constraint)null);
			assertEquals(0, store1.count(null, table, (Constraint)null));
		}
	}
	
	@Test
	public void gettingEmptyObjectAndGetItFromBothStores() throws IOException {
		this.truncateTable("t1");
		
		store1.storeChanges(null, null, "t1", "idt1", null, null, null);
		assertTrue(store2.exists(null, "t1", "idt1"));
		assertTrue(store1.exists(null, "t1", "idt1"));
	}
	
	@Test//(timeout=60000)
	public void creatingNewTableFrom2Threads() throws Throwable {
		final int [] done = new int[] {2};
		this.deleteTable("t1");
		PutElement r = new PutElement(done, store1);
		Thread t1 = new Thread(r, "Put 1");
		Thread t2 = new Thread(r, "Put 2");
		t1.start();t2.start();
		r.go();
		while (done[0] != 0) {
			Thread.sleep(10);
		}
		
		if (r.getError() != null)
			throw r.getError();
		assertTrue(store1.exists(null, "t1", "idt1"));
	}
	
	@Test(timeout=60000)
	public void creatingNewTableFrom2Stores() throws Throwable {
		final int [] done = new int[] {2};
		this.deleteTable("t1");
		PutElement r1 = new PutElement(done, store1); r1.go();
		PutElement r2 = new PutElement(done, store2); r2.go();
		Thread t1 =new Thread(r1, "Put from store1");
		Thread t2 =new Thread(r2, "Put from store2");
		t1.start();t2.start();
		while (done[0] != 0) {
			Thread.sleep(10);
		}

		if (r1.getError() != null)
			throw r1.getError();
		if (r2.getError() != null)
			throw r2.getError();
		assertTrue(store1.exists(null, "t1", "idt1"));
	}
	
	@Test(timeout=60000)
	public void creatingNewCFFrom2Threads() throws Throwable {
		final int [] done = new int[] {2};
		this.deleteTable("t1");
		store1.storeChanges(null, null, "t1", "idt1", null , null, null); //Creates T1 table with prop
		PutElement r = new PutElement(done, store1);
		Thread t1 = new Thread(r, "Put 1");
		Thread t2 = new Thread(r, "Put 2");
		t1.start();t2.start();
		r.go();
		while (done[0] != 0) {
			Thread.sleep(10);
		}

		if (r.getError() != null)
			throw r.getError();
		assertTrue(store1.exists(null, null, "t1", "idt1", "cf"));
	}
	
	@Test(timeout=60000)
	public void creatingNewCFFrom2Stores() throws Throwable {
		final int [] done = new int[] {2};
		this.deleteTable("t1");
		store1.storeChanges(null, null, "t1", "idt1", null , null, null); //Creates T1 table with prop
		store2.storeChanges(null, null, "t1", "idt1", null , null, null); //Creates T1 table with prop
		PutElement r1 = new PutElement(done, store1);
		PutElement r2 = new PutElement(done, store2);
		Thread t1 =new Thread(r1, "Put from store1");
		Thread t2 =new Thread(r2, "Put from store2");
		t1.start();t2.start();
		r1.go(); r2.go();
		while (done[0] != 0) {
			Thread.sleep(10);
		}

		if (r1.getError() != null)
			throw r1.getError();
		if (r2.getError() != null)
			throw r2.getError();
		assertTrue(store1.exists(null, "t1", "idt1"));
	}
	
	@Test
	public void acceptingOutsideTableRemoval() throws IOException {
		this.deleteTable("t1");
		
		ColumnFamilyData change1 = new DefaultColumnFamilyData();
		TreeMap<String, byte[]> ch1 = new TreeMap<String, byte[]>();
		change1.put("cf1", ch1);
		ch1.put("k1", new byte[]{1, 2});
		store1.storeChanges(null, null, "t1", "idt1", change1 , null, null); //Table should be created
		store2.storeChanges(null, null, "t1", "idt1", change1 , null, null); //Table should be discovered
		assertTrue(store2.exists(null, null, "t1", "idt1", "cf1"));
		
		this.deleteTable("t1");
		
		store1.storeChanges(null, null, "t1", "idt1", change1 , null, null); //Table should be re-discovered
		store2.storeChanges(null, null, "t1", "idt1", change1 , null, null); //Table should be re-discovered
		assertTrue(store2.exists(null, null, "t1", "idt1", "cf1")); 
	}
	
	@Test
	public void acceptingOutsideTableDisable() throws IOException {
		this.deleteTable("t1");
		
		ColumnFamilyData change1 = new DefaultColumnFamilyData();
		TreeMap<String, byte[]> ch1 = new TreeMap<String, byte[]>();
		change1.put("cf1", ch1);
		ch1.put("k1", new byte[]{1, 2});
		store1.storeChanges(null, null, "t1", "idt1", change1 , null, null); //Table should be created
		store2.storeChanges(null, null, "t1", "idt1", change1 , null, null); //Table should be discovered
		assertTrue(store2.exists(null, null, "t1", "idt1", "cf1"));
		
		this.disableTable("t1");
		
		store1.storeChanges(null, null, "t1", "idt1", change1 , null, null); //Table should be re-discovered
		store2.storeChanges(null, null, "t1", "idt1", change1 , null, null); //Table should be re-discovered
		assertTrue(store2.exists(null, null, "t1", "idt1", "cf1")); 
	}
	
	@Test(expected=Test.None.class)
	public void acceptingConnectionTimeout() throws IOException, SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		store1.delete(null, "t1", "idt1");
		
		ColumnFamilyData change1 = new DefaultColumnFamilyData();
		TreeMap<String, byte[]> ch1 = new TreeMap<String, byte[]>();
		change1.put("cf1", ch1);
		ch1.put("k1", new byte[]{1, 2});
		
		HConnection cm = store1.getAdmin().getConnection();
		Method closeM = cm.getClass().getDeclaredMethod("close", boolean.class);
		closeM.setAccessible(true);
		closeM.invoke(cm, true);
		
		store1.storeChanges(null, null, "t1", "idt1", change1 , null, null);
		assertTrue(store1.exists(null, null, "t1", "idt1", "cf1"));
		
		cm = store1.getAdmin().getConnection();
		closeM = cm.getClass().getDeclaredMethod("close", boolean.class);
		closeM.setAccessible(true);
		closeM.invoke(cm, true);
		
		store1.delete(null, "t1", "idt1");
		assertFalse(store1.exists(null, null, "t1", "idt1", "cf1"));
		
		store1.restart();
	}
	
	@Test(expected=Test.None.class)
	public void acceptingTableDisableWhileScanning() throws IOException, SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		this.truncateTable("t1");
		store1.setScanCaching(1);
		
		ColumnFamilyData change1 = new DefaultColumnFamilyData();
		TreeMap<String, byte[]> ch1 = new TreeMap<String, byte[]>();
		change1.put("cf1", ch1);
		ch1.put("k1", new byte[]{1, 2});
		store1.storeChanges(null, null, "t1", "idt1", change1 , null, null);
		store1.storeChanges(null, null, "t1", "idt2", change1 , null, null);
		assertTrue(store1.exists(null, null, "t1", "idt1", "cf1"));
		assertTrue(store1.exists(null, null, "t1", "idt2", "cf1"));
		
		Map<String, Field> change1Fams = new TreeMap<String, Field>();
		for (String fam : change1.keySet()) {
			change1Fams.put(fam, null);
		}

		CloseableKeyIterator it = store1.get(null, "t1", (Constraint)null, 100, change1Fams);
		
		try {
		
			store1.getAdmin().disableTable("t1");
			
			assertTrue(it.hasNext());
			assertEquals("idt1", it.next().getKey());
			
			assertTrue(it.hasNext());
			store1.getAdmin().disableTable("t1");
			assertEquals("idt2", it.next().getKey());
	
			store1.getAdmin().disableTable("t1");
			
			assertFalse(it.hasNext());
		
		} finally {
			it.close();
		}
	}
	
	@Test
	public void acceptingOutsideColumnFamilyAddition() throws IOException {

		this.deleteTable("t1");
		
		ColumnFamilyData change1 = new DefaultColumnFamilyData();
		TreeMap<String, byte[]> ch1 = new TreeMap<String, byte[]>();
		change1.put("cf1", ch1);
		ch1.put("k1", new byte[]{1, 2});
		store1.storeChanges(null, null, "t1", "idt1", change1 , null, null);
		store2.storeChanges(null, null, "t1", "idt1", change1 , null, null);
		
		ColumnFamilyData change2 = new DefaultColumnFamilyData();
		TreeMap<String, byte[]> ch2 = new TreeMap<String, byte[]>();
		change2.put("cf2", ch2);
		ch2.put("k1", new byte[]{1, 2});
		
		store1.storeChanges(null, null, "t1", "idt1", change2 , null, null); //CF cf2 should be added to table
		store2.storeChanges(null, null, "t1", "idt1", change2 , null, null); //CF cf2 should be discovered as added to table
		assertTrue(store2.exists(null, null, "t1", "idt1", "cf2"));
	}
	
	@Test(timeout=60000)
	public void acceptingOutsideColumnFamilyRemoval() throws IOException, InterruptedException {
		
		ColumnFamilyData change1 = new DefaultColumnFamilyData();
		TreeMap<String, byte[]> ch1 = new TreeMap<String, byte[]>();
		change1.put("cf1", ch1);
		ch1.put("k1", new byte[]{1, 2});
		store1.storeChanges(null, null, "t1", "idt1", change1 , null, null);
		
		byte[] tblNameBytes = Bytes.toBytes("t1");
		HTableDescriptor td = store1.getAdmin().getTableDescriptor(tblNameBytes);
		td.removeFamily(Bytes.toBytes("cf1"));
		store1.getAdmin().disableTable(tblNameBytes);
		store1.getAdmin().modifyTable(tblNameBytes, td);
		store1.getAdmin().enableTable(tblNameBytes);
		synchronized(this) {
			do {
				this.wait(500);
			} while (store1.getAdmin().getTableDescriptor(tblNameBytes).hasFamily(Bytes.toBytes("cf1")));
		}
		
		ColumnFamilyData change2 = new DefaultColumnFamilyData();
		TreeMap<String, byte[]> ch2 = new TreeMap<String, byte[]>();
		change2.put("cf1", ch2);
		ch2.put("k1", new byte[]{1, 2, 3});
		
		store1.storeChanges(null, null, "t1", "idt1", change2 , null, null); //CF cf2 should be added again to table
		assertTrue(store1.exists(null, null, "t1", "idt1", "cf1"));
	}
	
	@Test
	public void testCompressionForced() throws Exception {
		try {
			//Setting compression to none forced for both store
			store1.setForceCompression(true);
			store2.setForceCompression(true);
			store1.setCompression("none");
			store2.setCompression("none");
			store1.storeChanges(null, null, "t1", "row", null, null, null);
			store2.storeChanges(null, null, "t1", "row", null, null, null);
			HColumnDescriptor propFamD = store1.getAdmin().getTableDescriptor(Bytes.toBytes("t1")).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
			assertEquals(Algorithm.NONE, propFamD.getCompression());
			
			//Then setting GZ compression
			store1.setCompression("gz");
			store2.setCompression("gz");
			//Store1 should alter the table after a store request
			store1.storeChanges(null, null, "t1", "row", null, null, null);
			propFamD = store1.getAdmin().getTableDescriptor(Bytes.toBytes("t1")).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
			assertEquals(Algorithm.GZ, propFamD.getCompression());
			
			//Thread to check that table t1 is not disabled while storing changes from store2 does not alter t1
			final Object [] disableCheckerParameters = new Object[] {true, true, null}; //[0]=>always found enabled ; [1]=>should continue
			Thread disableChecker = new Thread() {

				@Override
				public void run() {
					HBaseAdmin admin = store1.getAdmin();
					byte[] tableName = Bytes.toBytes("t1");
					while ((Boolean)disableCheckerParameters[0] && (Boolean)disableCheckerParameters[1]) {
						try {
							disableCheckerParameters[0]=admin.isTableEnabled(tableName);
							Thread.sleep(10);
						} catch (Exception e) {
							disableCheckerParameters[2] = e;
							disableCheckerParameters[1] = false;
						}
					}
				}
				
			};
			disableChecker.start();
			
			//Test objective
			store2.storeChanges(null, null, "t1", "row", null, null, null);
			
			disableCheckerParameters[1] = false;
			disableChecker.join();
			//No exception thrown by the thread
			if (disableCheckerParameters[2] != null)
				throw (Exception)disableCheckerParameters[2];
			//Table was always checked as enabled
			assertTrue("Table was disabled to change compressor while compressor was already changed by another store", (Boolean)disableCheckerParameters[0]);
			//and is still in GZ mode
			propFamD = store1.getAdmin().getTableDescriptor(Bytes.toBytes("t1")).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
			assertEquals(Algorithm.GZ, propFamD.getCompression());
		} finally {
			store1.setForceCompression(false);
			store2.setForceCompression(false);
			store1.setCompression("none");
			store2.setCompression("none");
		}
	}
	
	@Test(expected=Test.None.class)
	public void connectionClosedAfterRestart() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {

		store1.restart();
		
		HConnection cm = store1.getAdmin().getConnection();
		Method closeM = cm.getClass().getDeclaredMethod("close", boolean.class);
		closeM.setAccessible(true);
		closeM.invoke(cm, true);
		
		store1.get(null, null, "t1", "row", PropertyManagement.PROPERTY_COLUMNFAMILY_NAME);
		
		assertTrue(store1.exists(null, "t1", "row"));
	}
	
	//@Test(expected=DatabaseNotReachedException.class)
	@Test
	public void requestTimeout() throws Throwable {
//		ExecutorService es = Executors.newCachedThreadPool();
//		try {
			store1.setClientTimeout(1);
			store1.restart();

			assertEquals("1", store1.getConf().get(HConstants.HBASE_RPC_TIMEOUT_KEY));
			assertEquals("1", store1.getAdmin().getConfiguration().get(HConstants.HBASE_RPC_TIMEOUT_KEY));
			assertEquals("1", store1.getAdmin().getConnection().getConfiguration().get(HConstants.HBASE_RPC_TIMEOUT_KEY));
			
			//Cannot reliably fail the connection (has to respond 4 times consequently in less than a ms)
//			final Throwable [] error = {null /*the expected exception*/, null /*any unexpected exception*/};
//	
//			//Overkilling the connection
//			for (int i = 0; i < 1000; ++i)
//				es.submit(new Runnable() {
//
//					@Override
//					public void run() {
//						try {
//							store1.storeChanges("t1", "row", null, null, null);
//							store1.get("t1", "row", PropertyManagement.PROPERTY_COLUMNFAMILY_NAME);
//						} catch (DatabaseNotReachedException t) {
//							error[0] = t;
//						} catch (Throwable t) {
//							error[1] = t;
//						}
//					}
//					
//				});
//			es.shutdown();
//			es.awaitTermination(10, TimeUnit.SECONDS);
//			assertNull(error[1]);
//			if (error[0] != null)
//				throw error[0];
//		} catch (Exception x) {
//			x.printStackTrace();
//		} finally {
//			store1.setClientTimeout(null);
//			store1.restart();
//		}
	}
}
