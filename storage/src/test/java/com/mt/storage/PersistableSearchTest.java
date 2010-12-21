package com.mt.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(Parameterized.class)
public class PersistableSearchTest extends StoreTestLauncher {
	@Parameters
	public static Collection<Object[]> testedStores() throws Exception {
		return StoreTestLauncher.getTestedStores();
	}
	
	public boolean isMemory;
	
	public PersistableSearchTest(Properties props) throws Exception {
		super(props);
		
	}
	
	@Persisting(table="PersistableSearch")
	public static class SUTClass {
		@Key public final int key1;
		@Key(order=2) public final int key2;
		
		public int dummyVar;
		
		public SUTClass(int key1, int key2) {
			this.key1 = key1;
			this.key2 = key2;
		}
		
		public boolean equals(Object rhs) {
			return rhs != null && rhs.getClass().equals(SUTClass.class) && ((SUTClass)rhs).key1 == this.key1 && ((SUTClass)rhs).key2 == this.key2;
		}
		
		
	}
	
	@Before public void createSuts() {
		try {
			if (new SUTClass(1, 1).existsInStore())
				return;
		} catch (Exception e1) {
			throw new RuntimeException(e1);
		}
		SUTClass s;
		for(int i = 0; i <= 100; ++i) {
			for(int j = 0; j <= 10; ++j) {
				s = new SUTClass(i, j);
				try {
					s.store();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
	
	@Test public void searchInexistingSutsWithNegativeFirstKey() throws DatabaseNotReachedException {
		Set<SUTClass> res = StorageManagement.findElement(SUTClass.class, new Constraint(SUTClass.class, null, "key1", -10, -5), 50);
		assertEquals(0, res.size());
	}
	
	@Test public void searchInexistingSutsWithFirstKey() throws DatabaseNotReachedException {
		Set<SUTClass> res = StorageManagement.findElement(SUTClass.class, new Constraint(SUTClass.class, null, "key1", 150, null), 50);
		assertEquals(0, res.size());
	}
	
	@Test public void searchSutsWithFirstKey() throws DatabaseNotReachedException {
		Set<SUTClass> res = StorageManagement.findElement(SUTClass.class, new Constraint(SUTClass.class, null, "key1", 49, 55), 1000);
		assertEquals((55-49+1)*11, res.size());
		for (SUTClass sutClass : res) {
			assertTrue(49 <= sutClass.key1 && sutClass.key1 <= 55);
		}
	}
	
	@Test public void search50SutsWithFirstKey() throws DatabaseNotReachedException {
		Set<SUTClass> res = StorageManagement.findElement(SUTClass.class, new Constraint(SUTClass.class, null, "key1", 49, 55), 50);
		assertEquals(50, res.size());
	}
	
	@Test public void search1SutWithFirstKey() throws DatabaseNotReachedException {
		Set<SUTClass> ress = StorageManagement.findElement(SUTClass.class, new Constraint(SUTClass.class, null, "key1", 49, 55), 1);
		assertEquals(1, ress.size());
		SUTClass res = ress.iterator().next();
		assertTrue(49 <= res.key1 && res.key1 <= 55);
	}
	
	@Test public void search1SutWithFixedFirstKey() throws DatabaseNotReachedException {
		Set<SUTClass> ress = StorageManagement.findElement(SUTClass.class, new Constraint(SUTClass.class, null, "key1", 49, 49), 1);
		assertEquals(1, ress.size());
		SUTClass res = ress.iterator().next();
		assertEquals(49, res.key1);
	}
	
	@Test public void searchSutsWithSecondKey() throws DatabaseNotReachedException {
		Map<String, Object> k1Val = new TreeMap<String, Object>();
		k1Val.put("key1", 35);
		Set<SUTClass> res = StorageManagement.findElement(SUTClass.class, new Constraint(SUTClass.class, k1Val, "key2", 5, 7), 1000);
		List<Integer> toBeFound = Arrays.asList(new Integer [] {5, 6, 7});
		assertEquals(3, res.size());
		for (SUTClass ret : res) {
			assertEquals(35, ret.key1);
			assertTrue(toBeFound.contains(ret.key2));
		}
	}
	
	@Test public void searchSutsWithSecondKeyNoUpper() throws DatabaseNotReachedException {
		Map<String, Object> k1Val = new TreeMap<String, Object>();
		k1Val.put("key1", 35);
		Set<SUTClass> res = StorageManagement.findElement(SUTClass.class, new Constraint(SUTClass.class, k1Val, "key2", 5, null), 1000);
		List<Integer> toBeFound = Arrays.asList(new Integer [] {5, 6, 7, 8, 9, 10});
		assertEquals(6, res.size());
		for (SUTClass ret : res) {
			assertEquals(35, ret.key1);
			assertTrue(toBeFound.contains(ret.key2));
		}
	}
	
	@Test public void searchSutsWithSecondKeyNoLower() throws DatabaseNotReachedException {
		Map<String, Object> k1Val = new TreeMap<String, Object>();
		k1Val.put("key1", 35);
		Set<SUTClass> res = StorageManagement.findElement(SUTClass.class, new Constraint(SUTClass.class, k1Val, "key2", null, 5), 1000);
		List<Integer> toBeFound = Arrays.asList(new Integer [] {5, 4, 3, 2, 1, 0});
		assertEquals(6, res.size());
		for (SUTClass ret : res) {
			assertEquals(35, ret.key1);
			assertTrue(toBeFound.contains(ret.key2));
		}
	}
}
