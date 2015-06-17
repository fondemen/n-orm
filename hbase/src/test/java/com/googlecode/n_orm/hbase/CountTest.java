package com.googlecode.n_orm.hbase;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.hbase.Store;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;

public class CountTest {
	private static Store store;
	private static final TableName testTable = TableName.valueOf("testtable");
	
	
	@BeforeClass
	public static void prepareStore() {
		HBaseLauncher.prepareHBase();
		store = HBaseLauncher.hbaseStore;
	}
	
	private Admin admin;
	
	@Before
	@After
	public void truncateTestTable() throws IOException {
		if (admin != null) {
			admin.close();
		}
		admin = store.getConnection().getAdmin();

		if (admin.tableExists(testTable)) {
			store.truncate(null, testTable.getNameAsString(), (Constraint)null);
			assertEquals(0, store.count(null, testTable.getNameAsString(), (Constraint)null));
		}
	}
	
	public void deleteTestTable() throws IOException {

		if (admin.tableExists(testTable)) {
			admin.disableTable(testTable);
			admin.deleteTable(testTable);
		}
	}

	@Test
	public void inexistingTable() throws IOException {
		assertEquals(0l, store.count(null, "huidhzidxeozd", (Constraint)null));
		
	}

	@Test
	public void none() {
		ColumnFamilyData change = new DefaultColumnFamilyData();
		Map<String, byte[]> famChange = new TreeMap<String, byte[]>();
		famChange.put("col", new byte[]{1, 2, 3});
		change.put("fam", famChange );
		store.storeChanges(null, testTable.getNameAsString(), "testid", change , null, null);
		store.delete(null, testTable.getNameAsString(), "testid");
		assertEquals(0l, store.count(null, testTable.getNameAsString(), (Constraint)null));
		
	}

	@Test
	public void oneEmpty() {
		store.storeChanges(null, testTable.getNameAsString(), "testid", null, null, null);
		assertEquals(1l, store.count(null, testTable.getNameAsString(), (Constraint)null));
		
	}

	@Test
	public void one() {
		ColumnFamilyData change = new DefaultColumnFamilyData();
		Map<String, byte[]> famChange = new TreeMap<String, byte[]>();
		famChange.put("col1", new byte[]{1, 2, 3});
		famChange.put("col2", new byte[]{1, 2, 3});
		change.put("fam1", famChange );
		change.put("fam2", famChange );
		store.storeChanges(null, testTable.getNameAsString(), "testid", change , null, null);
		assertEquals(1l, store.count(null, testTable.getNameAsString(), (Constraint)null));
		
	}
	
	@Test
	public void two() {
		ColumnFamilyData change = new DefaultColumnFamilyData();
		Map<String, byte[]> famChange = new TreeMap<String, byte[]>();
		famChange.put("col", new byte[]{1, 2, 3});
		change.put("fam", famChange );
		store.storeChanges(null, testTable.getNameAsString(), "testid", change , null, null);
		store.storeChanges(null, testTable.getNameAsString(), "testid2", null , null, null);
		assertEquals(2l, store.count(null, testTable.getNameAsString(), (Constraint)null));
	}
	
	@Test
	public void hundred() {
		for(int i = 0 ; i < 100; ++i) {
			store.storeChanges(null, testTable.getNameAsString(), "testid"+i, null , null, null);
		}
		assertEquals(100l, store.count(null, testTable.getNameAsString(), (Constraint)null));
	}
	
	@Test
	public void hundredFrom33to66() {
		for(int i = 0 ; i < 100; ++i) {
			store.storeChanges(null, testTable.getNameAsString(), ConversionTools.convertToString(i), null , null, null);
		}
		assertEquals(1+66-33, store.count(null, testTable.getNameAsString(), new Constraint(ConversionTools.convertToString(33), ConversionTools.convertToString(66))));
	}
	
	@Test
	public void thousandsFrom33to66() {
		for(int i = 0 ; i < 10000; ++i) {
			store.storeChanges(null, testTable.getNameAsString(), ConversionTools.convertToString(i), null , null, null);
		}
		assertEquals(1+66-33, store.count(null, testTable.getNameAsString(), new Constraint(ConversionTools.convertToString(33), ConversionTools.convertToString(66))));
	}
}
