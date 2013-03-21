package com.googlecode.n_orm.hbase;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;

public class MapRedTest {
	
	private static Store store;
	private static String tableName;
	private static HTable table;
	
	@BeforeClass
	public static void prepareStore() throws IOException {
		HBaseLauncher.prepareHBase();
		store = HBaseLauncher.hbaseStore;
		tableName = "t1";
		if (!store.getAdmin().tableExists(tableName)) {
			HTableDescriptor td = new HTableDescriptor(tableName);
			td.addFamily(new HColumnDescriptor(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
			store.getAdmin().createTable(td);
		}
		if (store.getAdmin().isTableDisabled(tableName)) {
			store.getAdmin().enableTable(tableName);
		}
		table = new HTable(store.getConf(), tableName);
		
		store.setCountMapRed(true);
		store.setTruncateMapRed(true);
		//store.setMapRedSendJars(false);
	}
	
	@AfterClass
	public static void resetStore() {
		store.setCountMapRed(false);
		store.setTruncateMapRed(false);
	}
	
	@Before
	@After
	public void truncate() throws IOException {
		store.setTruncateMapRed(false);
		store.truncate(null, tableName, (Constraint)null);
		assertEquals(0, store.count(null, tableName, (Constraint)null));
		store.setTruncateMapRed(true);
		
		Scan scan = new Scan();
		FilterList f = new FilterList();
		f.addFilter(new KeyOnlyFilter());
		f.addFilter(new FirstKeyOnlyFilter());
		scan.setFilter(f);
		scan.addFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		ResultScanner res = table.getScanner(scan);
		try {
			assertNull(res.next());
		} finally {
			res.close();
		}
	}
	
	protected long count() {
		store.setCountMapRed(false);
		long ret = store.count(null, tableName, (Constraint)null);
		store.setCountMapRed(true);
		return ret;
	}
	
	protected void createElements(int number) {
		for (int i = number; i > 0; --i) {
			ColumnFamilyData changed = new DefaultColumnFamilyData();
			Map<String, byte[]> changes = new TreeMap<String, byte[]>();
			changes.put("aval", new byte[] {1, 2, 3, 4});
			changed.put(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, changes );
			store.storeChanges(null, tableName, UUID.randomUUID().toString(), changed, null, null);
		}
	}

	@Test
	public void count0() {
		assertEquals(0, store.count(null, tableName, (Constraint)null));
	}
	
	@Test
	public void count1() {
		this.createElements(1);
		assertEquals(1, store.count(null, tableName, (Constraint)null));
	}
	
	@Test
	public void count100() {
		this.createElements(100);
		assertEquals(100, store.count(null, tableName, (Constraint)null));
	}
	
	@Test
	public void truncate0() {
		store.truncate(null, tableName, (Constraint)null);
	}
	
	@Test
	public void truncate1() {
		this.createElements(1);
		store.truncate(null, tableName, (Constraint)null);
	}
	
	@Test
	public void truncate100() {
		this.createElements(100);
		store.truncate(null, tableName, (Constraint)null);
	}

}
