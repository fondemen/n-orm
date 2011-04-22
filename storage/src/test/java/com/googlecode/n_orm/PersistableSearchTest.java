package com.googlecode.n_orm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.storeapi.Constraint;


public class PersistableSearchTest {
	
	public PersistableSearchTest() throws Exception {
		StoreTestLauncher.registerStorePropertiesForInnerClasses(getClass());
	}
	
	@Persisting(table="PersistableSearch")
	public static class SUTClass {
		@Key public int key1;
		@Key(order=2) public int key2;
		
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
				s.dummyVar = i + j;
				try {
					s.store();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
		 
		 //Simulates a new session by emptying elements cache
		 KeyManagement.getInstance().cleanupKnownPersistingElements();
	}

	public void checkOrder(Set<? extends PersistingElement> elements) {
		PersistingElement last = null;
		 for (PersistingElement elt : elements) {
			if (last != null) assertTrue(last.compareTo(elt) <= 0);
			last = elt;
		}
	}
	
	@Test public void searchInexistingSutsWithNegativeFirstKey() throws DatabaseNotReachedException {
		CloseableIterator<SUTClass> res = StorageManagement.findElement(SUTClass.class, new Constraint(SUTClass.class, null, "key1", -10, -5), 50, (String[])null);
		assertFalse(res.hasNext());
	}
	
	@Test public void searchInexistingSutsWithFirstKey() throws DatabaseNotReachedException {
		CloseableIterator<SUTClass> res = StorageManagement.findElement(SUTClass.class, new Constraint(SUTClass.class, null, "key1", 150, null), 50, (String[])null);
		assertFalse(res.hasNext());
	}
	
	@Test public void searchSutsWithFirstKey() throws DatabaseNotReachedException {
		Set<SUTClass> res = StorageManagement.findElementsToSet(SUTClass.class, new Constraint(SUTClass.class, null, "key1", 49, 55), 1000, (String[])null);
		assertEquals((55-49+1)*11, res.size());
		for (SUTClass sutClass : res) {
			assertTrue(49 <= sutClass.key1 && sutClass.key1 <= 55);
			assertEquals(0, sutClass.dummyVar);
		}
		checkOrder(res);
	}
	
	@Test public void searchSutsWithFirstKeyAndActivate() throws DatabaseNotReachedException {
		Set<SUTClass> res = StorageManagement.findElementsToSet(SUTClass.class, new Constraint(SUTClass.class, null, "key1", 49, 55), 1000, new String[0]);
		assertEquals((55-49+1)*11, res.size());
		for (SUTClass sutClass : res) {
			assertTrue(49 <= sutClass.key1 && sutClass.key1 <= 55);
			assertEquals(sutClass.key1+sutClass.key2, sutClass.dummyVar);
		}
		checkOrder(res);
	}
	
	@Test public void search50SutsWithFirstKey() throws DatabaseNotReachedException {
		Set<SUTClass> res = StorageManagement.findElementsToSet(SUTClass.class, new Constraint(SUTClass.class, null, "key1", 49, 55), 50, (String[])null);
		assertEquals(50, res.size());
		checkOrder(res);
	}
	
	@Test public void search1SutWithFirstKey() throws DatabaseNotReachedException {
		Set<SUTClass> ress = StorageManagement.findElementsToSet(SUTClass.class, new Constraint(SUTClass.class, null, "key1", 49, 55), 1, (String[])null);
		assertEquals(1, ress.size());
		SUTClass res = ress.iterator().next();
		assertTrue(49 <= res.key1 && res.key1 <= 55);
		assertEquals(0, res.dummyVar);
	}
	
	@Test public void search1SutWithFixedFirstKey() throws DatabaseNotReachedException {
		Set<SUTClass> ress = StorageManagement.findElementsToSet(SUTClass.class, new Constraint(SUTClass.class, null, "key1", 49, 49), 1, (String[])null);
		assertEquals(1, ress.size());
		SUTClass res = ress.iterator().next();
		assertEquals(49, res.key1);
	}
	
	@Test public void searchSutsWithSecondKey() throws DatabaseNotReachedException {
		Map<String, Object> k1Val = new TreeMap<String, Object>();
		k1Val.put("key1", 35);
		Set<SUTClass> res = StorageManagement.findElementsToSet(SUTClass.class, new Constraint(SUTClass.class, k1Val, "key2", 5, 7), 1000, (String[])null);
		List<Integer> toBeFound = Arrays.asList(new Integer [] {5, 6, 7});
		assertEquals(3, res.size());
		for (SUTClass ret : res) {
			assertEquals(35, ret.key1);
			assertTrue(toBeFound.contains(ret.key2));
		}
		checkOrder(res);
	}
	
	@Test public void searchSutsWithSecondKeyNoUpper() throws DatabaseNotReachedException {
		Map<String, Object> k1Val = new TreeMap<String, Object>();
		k1Val.put("key1", 35);
		Set<SUTClass> res = StorageManagement.findElementsToSet(SUTClass.class, new Constraint(SUTClass.class, k1Val, "key2", 5, null), 1000, (String[])null);
		List<Integer> toBeFound = Arrays.asList(new Integer [] {5, 6, 7, 8, 9, 10});
		assertEquals(6, res.size());
		for (SUTClass ret : res) {
			assertEquals(35, ret.key1);
			assertTrue(toBeFound.contains(ret.key2));
		}
		checkOrder(res);
	}
	
	@Test public void searchSutsWithSecondKeyNoLower() throws DatabaseNotReachedException {
		Map<String, Object> k1Val = new TreeMap<String, Object>();
		k1Val.put("key1", 35);
		Set<SUTClass> res = StorageManagement.findElementsToSet(SUTClass.class, new Constraint(SUTClass.class, k1Val, "key2", null, 5), 1000, (String[])null);
		List<Integer> toBeFound = Arrays.asList(new Integer [] {5, 4, 3, 2, 1, 0});
		assertEquals(6, res.size());
		for (SUTClass ret : res) {
			assertEquals(35, ret.key1);
			assertTrue(toBeFound.contains(ret.key2));
		}
		checkOrder(res);
	}
	
	@Persisting(table="PersistableSearchComposite")
	public static class CompositeSUT {
		@Key(order=1) SUTClass sut1;
		@Key(order=2) SUTClass sut2;
	}
	
	@Test
	public void seachWithDiveFirst() {
		CompositeSUT sut = new CompositeSUT();
		SUTClass sut1 = new SUTClass(11, 12);
		SUTClass sut2 = new SUTClass(21, 22);
		sut.sut1 = sut1;
		sut.sut2 = sut2;
		sut.store();
		KeyManagement.getInstance().cleanupKnownPersistingElements();
		
		Constraint c = new Constraint(CompositeSUT.class, null, "sut1", new Constraint(SUTClass.class, null, "key1", 11, 11));
		
		Set<CompositeSUT> found = StorageManagement.findElementsToSet(CompositeSUT.class, c, 100);
		assertEquals(1, found.size());
		assertEquals(sut, found.iterator().next());
	}
	
	@Test
	public void negativeSeachWithDiveFirst() {
		CompositeSUT sut = new CompositeSUT();
		SUTClass sut1 = new SUTClass(11, 12);
		SUTClass sut2 = new SUTClass(21, 22);
		sut.sut1 = sut1;
		sut.sut2 = sut2;
		sut.store();
		KeyManagement.getInstance().cleanupKnownPersistingElements();
		
		Constraint c = new Constraint(CompositeSUT.class, null, "sut1", new Constraint(SUTClass.class, null, "key1", 12, 15));
		
		Set<CompositeSUT> found = StorageManagement.findElementsToSet(CompositeSUT.class, c, 100);
		assertEquals(0, found.size());
	}
	
	@Test
	public void seachWithDiveSecondFirst() {
		CompositeSUT sut = new CompositeSUT();
		SUTClass sut1 = new SUTClass(11, 12);
		SUTClass sut2 = new SUTClass(21, 22);
		sut.sut1 = sut1;
		sut.sut2 = sut2;
		sut.store();
		KeyManagement.getInstance().cleanupKnownPersistingElements();

		Map<String, Object> k1Val = new TreeMap<String, Object>();
		k1Val.put("key1", 11);
		Constraint c = new Constraint(CompositeSUT.class, null, "sut1", new Constraint(SUTClass.class, k1Val, "key2", 12, 15));
		
		Set<CompositeSUT> found = StorageManagement.findElementsToSet(CompositeSUT.class, c, 100);
		assertEquals(1, found.size());
		assertEquals(sut, found.iterator().next());
	}
	
	@Test
	public void seachWithDiveSecondFirstNegativeForFirst() {
		CompositeSUT sut = new CompositeSUT();
		SUTClass sut1 = new SUTClass(11, 12);
		SUTClass sut2 = new SUTClass(21, 22);
		sut.sut1 = sut1;
		sut.sut2 = sut2;
		sut.store();
		KeyManagement.getInstance().cleanupKnownPersistingElements();

		Map<String, Object> k1Val = new TreeMap<String, Object>();
		k1Val.put("key1", 35);
		Constraint c = new Constraint(CompositeSUT.class, null, "sut1", new Constraint(SUTClass.class, k1Val, "key2", 12, 15));
		
		Set<CompositeSUT> found = StorageManagement.findElementsToSet(CompositeSUT.class, c, 100);
		assertEquals(0, found.size());
	}
	
	@Test
	public void seachWithDiveSecondFirstNegativeForSecond() {
		CompositeSUT sut = new CompositeSUT();
		SUTClass sut1 = new SUTClass(11, 12);
		SUTClass sut2 = new SUTClass(21, 22);
		sut.sut1 = sut1;
		sut.sut2 = sut2;
		sut.store();
		KeyManagement.getInstance().cleanupKnownPersistingElements();

		Map<String, Object> k1Val = new TreeMap<String, Object>();
		k1Val.put("key1", 11);
		Constraint c = new Constraint(CompositeSUT.class, null, "sut1", new Constraint(SUTClass.class, k1Val, "key2", 0, 10));
		
		Set<CompositeSUT> found = StorageManagement.findElementsToSet(CompositeSUT.class, c, 100);
		assertEquals(0, found.size());
	}
	
	@Test
	public void seachWithDiveSecondSecond() {
		CompositeSUT sut = new CompositeSUT();
		SUTClass sut1 = new SUTClass(11, 12);
		SUTClass sut2 = new SUTClass(21, 22);
		sut.sut1 = sut1;
		sut.sut2 = sut2;
		sut.store();
		KeyManagement.getInstance().cleanupKnownPersistingElements();
		
		Map<String, Object> sut1Val = new TreeMap<String, Object>();
		sut1Val.put("sut1", sut1);

		Map<String, Object> k1Val = new TreeMap<String, Object>();
		k1Val.put("key1", 21);
		Constraint c = new Constraint(CompositeSUT.class, sut1Val, "sut2", new Constraint(SUTClass.class, k1Val, "key2", 21, 25));
		
		Set<CompositeSUT> found = StorageManagement.findElementsToSet(CompositeSUT.class, c, 100);
		assertEquals(1, found.size());
		assertEquals(sut, found.iterator().next());
	}
	
	@Test
	public void seachWithDiveSecondSecondNegativeForFirst() {
		CompositeSUT sut = new CompositeSUT();
		SUTClass sut1 = new SUTClass(11, 12);
		SUTClass sut2 = new SUTClass(21, 22);
		sut.sut1 = sut1;
		sut.sut2 = sut2;
		sut.store();
		KeyManagement.getInstance().cleanupKnownPersistingElements();
		
		Map<String, Object> sut1Val = new TreeMap<String, Object>();
		sut1Val.put("sut1", sut1);

		Map<String, Object> k1Val = new TreeMap<String, Object>();
		k1Val.put("key1", 35);
		Constraint c = new Constraint(CompositeSUT.class, sut1Val, "sut2", new Constraint(SUTClass.class, k1Val, "key2", 22, 25));
		
		Set<CompositeSUT> found = StorageManagement.findElementsToSet(CompositeSUT.class, c, 100);
		assertEquals(0, found.size());
	}
	
	@Test
	public void seachWithDiveSecondNegativeForSecond() {
		CompositeSUT sut = new CompositeSUT();
		SUTClass sut1 = new SUTClass(11, 12);
		SUTClass sut2 = new SUTClass(21, 22);
		sut.sut1 = sut1;
		sut.sut2 = sut2;
		sut.store();
		KeyManagement.getInstance().cleanupKnownPersistingElements();
		
		Map<String, Object> sut1Val = new TreeMap<String, Object>();
		sut1Val.put("sut1", sut1);

		Map<String, Object> k1Val = new TreeMap<String, Object>();
		k1Val.put("key1", 21);
		Constraint c = new Constraint(CompositeSUT.class, sut1Val, "sut2", new Constraint(SUTClass.class, k1Val, "key2", 0, 20));
		
		Set<CompositeSUT> found = StorageManagement.findElementsToSet(CompositeSUT.class, c, 100);
		assertEquals(0, found.size());
	}
}
