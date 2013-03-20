package com.googlecode.n_orm;

import static org.junit.Assert.*;

import java.util.NavigableSet;

import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.cf.MapColumnFamily;
import com.googlecode.n_orm.memory.Memory;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.SimpleStoreWrapper;
import com.googlecode.n_orm.storeapi.Store;

public class FederatedTablesTest {
	public static final String key = "keym";
	
	public FederatedTablesTest() throws Exception {
		StoreTestLauncher.registerStorePropertiesForInnerClasses(getClass());
	}

	@Persisting(table = "t", federated = FederatedMode.RCONS)
	public static class Element {
		private static final long serialVersionUID = -6877770319760457398L;
		@Key
		public String key;
		public String post;
		public String arg;
		public MapColumnFamily<String, String> cf = new MapColumnFamily<String, String>();

		public String getTablePostfix() {
			return this.post;
		}
	}

	@Before
	public void cleanupCache() {
		ConsistentElement elt = new ConsistentElement();
		elt.key = key;

		boolean ok;
		do {
			try {
				elt.delete();
				ok = !elt.existsInStore();
			} catch (AssertionError x) { // Is element still exists
				x.printStackTrace();
				ok = false;
			}
		} while (!ok);

		// Mandatory as we are using different data stores for the same class in
		// this test case
		FederatedTableManagement.clearAlternativesCache();
	}
	
	@Before
	public void cleanupElement() {
		MergableElement e = new MergableElement();
		e.key = key;
		do {
			try {
				e.delete();
			} catch (AssertionError x) {}
			e = new MergableElement();
			e.key = key;
		} while (e.existsInStore());
	}

	@Before
	public void cleanupLists() {
		ConsistentElement elt = new ConsistentElement();
		elt.key = "key1";
		if (!elt.existsInStore())
			return;

		for (int i = 0; i < 100; ++i) {
			elt = new ConsistentElement();
			elt.key = "key" + i;
			elt.delete();
		}

		// Mandatory as we are using different data stores for the same class in
		// this test case
		FederatedTableManagement.clearAlternativesCache();
	}

	@Before
	public void cleanupAlpha() {
		ConsistentElement elt = new ConsistentElement();
		elt.key = "keya";
		if (!elt.existsInStore())
			return;

		for (int i = 'a'; i < 'z'; ++i) {
			elt = new ConsistentElement();
			elt.key = "key" + (char) i;
			elt.delete();
		}

		// Mandatory as we are using different data stores for the same class in
		// this test case
		FederatedTableManagement.clearAlternativesCache();
	}

	@Test
	public void sendingToTable() {
		Memory.INSTANCE.reset();

		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post";

		assertNull(Memory.INSTANCE.getRow("tpost", elt.getIdentifier(), false));
		assertEquals("t", elt.getTable());

		elt.store();

		assertNotNull(Memory.INSTANCE.getRow("tpost", elt.getIdentifier(),
				false));
		assertNull(Memory.INSTANCE.getRow("t", elt.getIdentifier(), false));
		assertEquals("tpost", elt.getActualTable());
	}

	@Test
	public void gettingFromGuessedTable() {
		FederatedTableManagement.clearAlternativesCache();
		Memory.INSTANCE.resetQueries();

		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post";
		elt.arg = "a value";
		elt.store();
		// A query to register the alternative table
		// Another one to actually store the element
		assertEquals(2, Memory.INSTANCE.getQueriesAndReset());

		FederatedTableManagement.clearAlternativesCache();

		Element elt2 = new Element();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = key;
		elt2.post = "post";
		elt2.activate();

		assertEquals(elt.arg, elt2.arg);
		// 1: checking for tpost existence
		// 2: updating alternatives table
		// 3: activating
		assertEquals(3, Memory.INSTANCE.getQueriesAndReset());
		assertEquals("tpost", elt2.getActualTable());
	}

	@Test
	public void tableAlternativeIsCached() throws InterruptedException {
		FederatedTableManagement.clearAlternativesCache();
		Memory.INSTANCE.reset();

		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post";
		elt.arg = "a value";
		elt.activate();
		// 1: checking in tpost (not found)
		// 2: updating alternative tables from base
		// 3: testing table t existence (not found)
		// No more table to test, does not exists, no activation
		assertEquals(3, Memory.INSTANCE.getQueriesAndReset());

		Element elt2 = new Element();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = "ljs,dcfzklfzkl";
		elt2.post = "postHGHJKNHJKHJK";
		elt2.activate();

		// 1: testing for table tpostHGHJKNHJKHJK existence
		// 2: testing table t -> NO: we already know from before whether table t exists
		// Not updating tables from base
		// No more table to test, does not exists, no activation
		assertEquals(1, Memory.INSTANCE.getQueriesAndReset());
	}

	@Test
	public void tableAlternativeIsCachedNotTooLong()
			throws InterruptedException {
		FederatedTableManagement.clearAlternativesCache();
		Memory.INSTANCE.reset();

		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post";
		elt.arg = "a value";
		elt.activate();
		// 1: testing table tpost
		// 2: updating alternative tables from base
		// 3: checking for legacy table
		// No more table to test, does not exists, no activation
		assertEquals(3, Memory.INSTANCE.getQueriesAndReset());

		long initialTTL = FederatedTableManagement
				.getTableAlternativeCacheTTLInS();
		FederatedTableManagement.setTableAlternativeCacheTTLInS(1);
		try {
			// Just to be sure update is needed
			Thread.sleep(2);

			Element elt2 = new Element();
			elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
			elt2.key = "ljs,dcfzklfzkl";
			elt2.post = "postHGHJKNdqsn,qHJKHJK";
			elt2.activate();

			assertEquals(elt.arg, elt2.arg);
			// 1: testing table tljs,dcfzklfzk
			// 2: updating alternative tables from store
			// 3: checking for legacy table
			// Not found, giving up
			assertEquals(3, Memory.INSTANCE.getQueriesAndReset());
		} finally {
			FederatedTableManagement.setTableAlternativeCacheTTLInS(initialTTL);
		}
	}

	@Test
	public void storeStoresAlternatives() throws InterruptedException {
		FederatedTableManagement.clearAlternativesCache();
		Memory.INSTANCE.reset();

		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post";
		elt.arg = "a value";
		elt.store();

		assertTrue(Memory.INSTANCE
				.getRow(FederatedTableManagement.FEDERATED_META_TABLE, "t",
						false)
				.get(FederatedTableManagement.FEDERATED_META_COLUMN_FAMILY)
				.contains("post"));
	}

	@Test
	public void activateDoesntStoreAlternatives() throws InterruptedException {
		FederatedTableManagement.clearAlternativesCache();
		Memory.INSTANCE.reset();

		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post";
		elt.arg = "a value";
		elt.activate();

		assertNull(Memory.INSTANCE.getRow(
				FederatedTableManagement.FEDERATED_META_TABLE, "t", false));
	}

	@Test
	public void deleteDoesntStoreAlternatives() throws InterruptedException {
		FederatedTableManagement.clearAlternativesCache();
		Memory.INSTANCE.reset();

		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post";
		elt.arg = "a value";
		elt.delete();

		assertNull(Memory.INSTANCE.getRow(
				FederatedTableManagement.FEDERATED_META_TABLE, "t", false));
	}

	@Test
	public void existsDoesntStoreAlternatives() throws InterruptedException {
		FederatedTableManagement.clearAlternativesCache();
		Memory.INSTANCE.reset();

		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post";
		elt.arg = "a value";
		elt.existsInStore();

		assertNull(Memory.INSTANCE.getRow(
				FederatedTableManagement.FEDERATED_META_TABLE, "t", false));
	}

	@Test
	public void gettingFromKnownTable() {
		Memory.INSTANCE.reset();
		FederatedTableManagement.clearAlternativesCache();

		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post";
		elt.arg = "a value";
		elt.store(); // Caches the tpost table as an alternative to t

		Memory.INSTANCE.resetQueries();

		Element elt2 = new Element();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = key;
		elt2.activate(); // tpost table can only be found from cache

		// 1: testing t
		// 2: testing tpost
		// 3: activation
		assertEquals(3, Memory.INSTANCE.getQueriesAndReset());
		assertEquals(elt.arg, elt2.arg);
		assertEquals("tpost", elt2.getActualTable());
	}

	@Test
	public void gettingFromUnknownTable() {
		Memory.INSTANCE.resetQueries();
		FederatedTableManagement.clearAlternativesCache();

		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post";
		elt.arg = "a value";
		elt.store(); // Caches the tpost table as an alternative to t

		Memory.INSTANCE.resetQueries();
		FederatedTableManagement.clearAlternativesCache(); // Forgets that tpost
															// actually exists

		Element elt2 = new Element();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = key;
		elt2.activate(); // tpost table can only be found from store

		// 1: testing t
		// 2: updating alternatives from cache
		// 3: checking for legacy table
		// 4: testing whether tpost is a table (found from cache)
		// 5: testing tpost
		// 6: activation
		assertEquals(6, Memory.INSTANCE.getQueriesAndReset());
		assertEquals(elt.arg, elt2.arg);
		assertEquals("tpost", elt2.getActualTable());
	}

	@Test
	public void gettingFromUnKnownTable() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post";
		elt.arg = "a value";
		elt.store(); // Stores the tpost table as an alternative to t in stored
						// metadata

		FederatedTableManagement.clearAlternativesCache(); // Forgets that tpost
															// actually exists

		Element elt2 = new Element();
		elt2.key = key;
		elt2.activate(); // tpost table can only be found from meta data

		assertEquals(elt.arg, elt2.arg);
		assertEquals("tpost", elt2.getActualTable());
	}

	@Test
	public void storeAndActivate() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post";
		elt.arg = "a value";
		elt.store();

		Element elt2 = new Element();
		elt2.key = key;
		elt2.activate();

		assertEquals(elt.post, elt2.post);
		assertEquals(elt.arg, elt2.arg);
		assertEquals("tpost", elt2.getActualTable());
	}

	@Test
	public void storeAndActivateIfNotAlready() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post";
		elt.arg = "a value";
		elt.store();

		Element elt2 = new Element();
		elt2.key = key;
		elt2.activateIfNotAlready();

		assertEquals(elt.post, elt2.post);
		assertEquals(elt.arg, elt2.arg);
		assertEquals("tpost", elt2.getActualTable());
	}

	@Test
	public void alreadyKnownPostfixYetHasChanged() {
		Memory.INSTANCE.reset();

		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post1";
		elt.store();

		assertNotNull(Memory.INSTANCE.getRow("tpost1", elt.getIdentifier(),
				false));
		assertNull(Memory.INSTANCE.getRow("t", elt.getIdentifier(), false));
		assertNull(Memory.INSTANCE.getRow("tpost2", elt.getIdentifier(), false));
		assertEquals("tpost1", elt.getActualTable());

		elt.post = "post2";
		elt.store();

		assertNotNull(Memory.INSTANCE.getRow("tpost1", elt.getIdentifier(),
				false));
		assertNull(Memory.INSTANCE.getRow("t", elt.getIdentifier(), false));
		assertNull(Memory.INSTANCE.getRow("tpost2", elt.getIdentifier(), false));
		assertEquals("tpost1", elt.getActualTable());
	}

	@Persisting(table = "t", federated = FederatedMode.PC_INCONS)
	public static class CheckedElement {
		private static final long serialVersionUID = 5367746075423856372L;
		@Key
		public String key;
		public String post;
		public String arg;

		public String getTablePostfix() {
			return this.post;
		}
	}

	@Test(expected = IllegalStateException.class)
	public void alreadyKnownPostfixYetHasChangedButCheckedStoreVersion() {
		Memory.INSTANCE.reset();

		CheckedElement elt = new CheckedElement();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post1";
		elt.store();

		elt.post = "post2";
		elt.store();
	}

	@Test(expected = IllegalStateException.class)
	public void alreadyKnownPostfixYetHasChangedButCheckedActivateVersion() {
		Memory.INSTANCE.reset();

		CheckedElement elt = new CheckedElement();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post1";
		elt.store(); // Stores key element in "tpost1"

		CheckedElement elt2 = new CheckedElement();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.post = "post2";
		elt2.activate(); // Finds key from "tpost1" while it should be found
							// only from "tpost2" !
	}

	@Persisting(table = "t", federated = FederatedMode.CONS)
	public static class ConsistentElement {
		private static final long serialVersionUID = -1514618678838564130L;
		@Key
		public String key;
		public String post;
		public String arg;

		public String getTablePostfix() {
			return this.post;
		}
	}

	@Test
	public void alreadyKnownPostfixYetHasChangedButConsistentActivateVersion() {
		Memory.INSTANCE.reset();

		ConsistentElement elt = new ConsistentElement();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post1";
		elt.store(); // Stores key element in "tpost1"

		FederatedTableManagement.clearAlternativesCache();

		ConsistentElement elt2 = new ConsistentElement();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = key;
		elt2.activate(); // Finds key from "tpost1" while it should be found
							// only from "t" !

		assertEquals(elt.arg, elt2.arg);
		assertEquals(elt.post, elt2.post);
		assertEquals(elt.getActualTable(), elt2.getActualTable());
	}

	@Test
	public void alreadyKnownPostfixYetHasChangedButConsistentStoreVersion() {
		Memory.INSTANCE.reset();

		ConsistentElement elt = new ConsistentElement();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.arg = "arg1";
		elt.post = "post1";
		elt.store(); // Stores key element in "tpost1"

		FederatedTableManagement.clearAlternativesCache();

		ConsistentElement elt2 = new ConsistentElement();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = key;
		elt2.arg = "arg2";
		elt2.post = "post2";
		elt2.store(); // Finds key from "tpost1" while it should be found only
						// from "tpost2" !

		elt.activate();
		assertEquals("arg2", elt.arg);
		assertEquals("post2", elt.post);
		assertEquals("tpost1", elt2.getActualTable());
	}

	@Persisting(table = "t", federated = FederatedMode.PC_LEG)
	public static class LegacyableElement {
		private static final long serialVersionUID = 1316553095510929588L;
		@Key
		public String key;
		public String post;
		public String arg;

		public String getTablePostfix() {
			return this.post;
		}
	}

	@Test
	public void notFromLegacy() {
		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post1";
		elt.store(); // Stores key element in "tpost1" (legacy)

		FederatedTableManagement.clearAlternativesCache();

		LegacyableElement elt2 = new LegacyableElement();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = key;
		elt2.post = "post2";
		elt2.store(); // Misses "tpost1" !!!

		assertEquals("tpost2", elt2.getActualTable());

		// We are in an INCONSISTENT state !
		ConsistentElement celt = new ConsistentElement();
		celt.key = elt.key;
		assertTrue(celt.existsInStore());

		elt.getStore().delete(null, "tpost1", elt.getIdentifier());
			
		try {
			assertFalse(elt.existsInStore()); // Deleted but found back from tpost2 ; can't change postfix
			//fail("Changed postfix");
		} catch (Exception x) {}
		assertTrue(celt.existsInStore());
		assertTrue(elt2.existsInStore());

		elt2.delete();
		assertFalse(elt.existsInStore());
		assertFalse(celt.existsInStore());
		assertFalse(elt2.existsInStore());

		elt2.delete();
		assertFalse(elt.existsInStore());
		assertFalse(celt.existsInStore());
		assertFalse(elt2.existsInStore());
	}

	@Test
	public void exists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post";

		elt.store();

		Element elt2 = new Element();
		elt2.key = key;

		assertTrue(elt2.existsInStore());
		assertTrue(elt.existsInStore());
	}

	@Test
	public void delete() {
		Memory.INSTANCE.reset();

		Element elt = new Element();
		elt.key = key;
		elt.post = "post";

		elt.store();

		assertTrue(elt.existsInStore());

		elt.delete();

		assertFalse(elt.existsInStore());
	}

	@Test
	public void deleteFromOutside() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post";

		elt.store();

		Element elt2 = new Element();
		elt2.key = key;

		elt2.delete();

		assertFalse(elt.existsInStore());
	}

	@Test
	public void deleteCompletelyUnknown() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post";

		elt.store();

		FederatedTableManagement.clearAlternativesCache();

		Element elt2 = new Element();
		elt2.key = key;

		elt2.delete();

		assertFalse(elt.existsInStore());
	}

	// =================== Migration tests ======================

	@Persisting(table = "t")
	public static class SimpleElement {
		private static final long serialVersionUID = -5742148285547881955L;
		@Key
		public String key;
		public String arg;
	}

	@Test
	public void elementFromStandardTableActivate() {
		SimpleElement elt = new SimpleElement();
		elt.key = key;
		elt.arg = "etrcauzjy";

		elt.store();

		Element elt2 = new Element();
		elt2.key = key;
		elt2.post = "post";
		elt2.activate();

		assertEquals("etrcauzjy", elt2.arg);
	}

	@Test
	public void elementFromStandardTableExists() {
		SimpleElement elt = new SimpleElement();
		elt.key = key;
		elt.arg = "etrcauzjy";

		elt.store();

		Element elt2 = new Element();
		elt2.key = key;
		elt2.post = "post";

		assertTrue(elt2.existsInStore());
	}

	@Test
	public void elementFromStandardTableNotExists() {
		SimpleElement elt = new SimpleElement();
		elt.key = key;
		elt.arg = "etrcauzjy";

		elt.delete();

		Element elt2 = new Element();
		elt2.key = key;
		elt2.post = "post";
		elt2.activate();

		assertFalse(elt2.existsInStore());
	}

	@Test
	public void elementFromStandardTableDelete() {
		SimpleElement elt = new SimpleElement();
		elt.key = key;
		elt.arg = "etrcauzjy";

		elt.store();

		Element elt2 = new Element();
		elt2.key = key;
		elt2.post = "post";
		elt2.delete();

		assertFalse(elt.existsInStore());
	}

	@Test
	public void existsCfWhenElementDoesNotExists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		// deleted by @Before method cleanupCache()

		assertTrue(elt.cf.isEmptyInStore());
		assertEquals("tpost1", elt.getActualTable());
	}

	@Test
	public void existsCfWhenCFDoesNotExists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		elt.store();

		Element elt2 = new Element();
		elt2.key = key;

		assertTrue(elt2.cf.isEmptyInStore());
		assertEquals("tpost1", elt2.getActualTable());
	}

	@Test
	public void existsCfWhenCFExists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		elt.cf.put("qual", "AZERTY");
		elt.store();

		Element elt2 = new Element();
		elt2.key = key;

		assertFalse(elt2.cf.isEmptyInStore());
		assertEquals("tpost1", elt2.getActualTable());
	}

	@Test
	public void getCfWhenElementDoesNotExists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		// deleted by @Before method cleanupCache()

		assertNull(elt.cf.getFromStore("qual"));
		assertEquals("tpost1", elt.getActualTable());
	}

	@Test
	public void getCfWhenCFDoesNotExists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		elt.store();

		Element elt2 = new Element();
		elt2.key = key;

		assertNull(elt2.cf.getFromStore("qual"));
		assertEquals("tpost1", elt2.getActualTable());
	}

	@Test
	public void getCfWhenCFExists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		elt.cf.put("qual", "AZERTY");
		elt.store();

		Element elt2 = new Element();
		elt2.key = key;

		assertEquals("AZERTY", elt2.cf.getFromStore("qual"));

		assertEquals("tpost1", elt2.getActualTable());
	}

	@Test
	public void getAllCfWhenElementDoesNotExists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		elt.cf.put("qual", "AZZERTYTEST");
		elt.cf.activate();
		// deleted by @Before method cleanupCache()

		assertEquals("tpost1", elt.getActualTable());

		assertTrue(elt.cf.isEmpty());

		// Testing again as table should already be known now and not go through
		// aspect
		elt.cf.activate();
		assertTrue(elt.cf.isEmpty());
	}

	@Test
	public void getAllCfWhenCFDoesNotExists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		elt.store();

		Element elt2 = new Element();
		elt2.key = key;
		elt2.cf.put("qual", "AZZERTYTEST");
		elt2.cf.activate();

		assertEquals("tpost1", elt2.getActualTable());

		assertTrue(elt2.cf.isEmpty());

		// Testing again as table should already be known now and not go through
		// aspect
		elt2.cf.activate();
		assertTrue(elt2.cf.isEmpty());
	}

	@Test
	public void getAllCfWhenCFExists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		elt.cf.put("qual", "AZERTY");
		elt.store();

		Element elt2 = new Element();
		elt2.key = key;
		elt2.cf.put("qual", "AZZERTYTEST");
		elt2.cf.activate();

		assertEquals("tpost1", elt2.getActualTable());

		assertEquals("AZERTY", elt2.cf.get("qual"));

		// Testing again as table should already be known now and not go through
		// aspect
		elt.cf.activate();
		assertEquals("AZERTY", elt.cf.get("qual"));
		elt2.cf.activate();
		assertEquals("AZERTY", elt2.cf.get("qual"));
	}

	@Test
	public void getAllConstrainedCfWhenElementDoesNotExists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		elt.cf.put("qual", "AZZERTYTEST");
		elt.cf.activate(new Constraint("A", "Z"));
		// deleted by @Before method cleanupCache()

		assertEquals("tpost1", elt.getActualTable());

		assertTrue(elt.cf.isEmpty());

		// Testing again as table should already be known now and not go through
		// aspect
		elt.cf.activate(new Constraint("A", "Z"));
		assertTrue(elt.cf.isEmpty());
	}

	@Test
	public void getAllConstrainedCfWhenCFDoesNotExists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		elt.store();

		Element elt2 = new Element();
		elt2.key = key;
		elt2.cf.put("qual", "AZZERTYTEST");
		elt2.cf.activate(new Constraint("A", "Z"));

		assertEquals("tpost1", elt2.getActualTable());

		assertTrue(elt2.cf.isEmpty());

		// Testing again as table should already be known now and not go through
		// aspect
		elt2.cf.activate(new Constraint("A", "Z"));
		assertTrue(elt2.cf.isEmpty());
	}

	@Test
	public void getAllConstrainedCfWhenCFExists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		elt.cf.put("qual", "AZERTY1");
		elt.cf.put("QUAL", "AZERTY2");
		elt.store();

		Element elt2 = new Element();
		elt2.key = key;
		elt2.cf.put("qual", "AZZERTYTEST");
		elt2.cf.activate(new Constraint("A", "Z"));

		assertEquals("tpost1", elt2.getActualTable());

		assertEquals("AZERTY2", elt2.cf.get("QUAL"));
		assertNull(elt2.cf.get("qual"));

		// Testing again as table should already be known now and not go through
		// aspect
		elt.cf.activate(new Constraint("A", "Z"));
		assertEquals("AZERTY2", elt.cf.get("QUAL"));
		assertNull(elt.cf.get("qual"));
		elt2.cf.activate(new Constraint("A", "Z"));
		assertEquals("AZERTY2", elt2.cf.get("QUAL"));
		assertNull(elt2.cf.get("qual"));
	}

	@Test
	public void countNone() {
		assertEquals(0, StorageManagement.findElements().ofClass(Element.class)
				.count());
	}

	@Test
	public void countAll() {
		for (int i = 0; i < 100; ++i) {
			Element elt = new Element();
			elt.key = "key" + i;
			int post = i % 4;
			if (post != 0)
				elt.post = "post" + post;
			elt.store();
		}

		assertEquals(100,
				StorageManagement.findElements().ofClass(Element.class).count());
	}

	@Test
	public void countAllFromLegacy() {
		Store store = null;
		for (int i = 0; i < 100; ++i) {
			Element elt = new Element();
			elt.key = "key" + i;
			elt.store();
			if (store == null)
				store = elt.getStore();
		}
		store.delete(null, FederatedTableManagement.FEDERATED_META_TABLE, PersistingMixin.getInstance().getTable(Element.class));
		
		FederatedTableManagement.clearAlternativesCache();

		assertEquals(100,
				StorageManagement.findElements().ofClass(Element.class).count());
	}

	@Test
	public void countAllFromLegacyWithPostAlreadyRegistered() {
		Store store = null;
		for (int i = 0; i < 100; ++i) {
			Element elt = new Element();
			elt.key = "key" + i;
			elt.store();
			if (store == null)
				store = elt.getStore();
		}
		store.delete(null, FederatedTableManagement.FEDERATED_META_TABLE, PersistingMixin.getInstance().getTable(Element.class));
		
		FederatedTableManagement.clearAlternativesCache();
		
		// Registers a brand new postfix (not the "" postfix)
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		elt.store();

		assertEquals(101,
				StorageManagement.findElements().ofClass(Element.class).count());
	}

	@Test
	public void countAllInTable() {
		for (int i = 0; i < 100; ++i) {
			Element elt = new Element();
			elt.key = "key" + i;
			int post = i % 4;
			if (post != 0)
				elt.post = "post" + post;
			elt.store();
		}

		assertEquals(25,
				StorageManagement.findElements().ofClass(Element.class).inTableWithPostfix("post1").count());
	}

	@Test
	public void countConstrained() {
		for (int i = 'a'; i < 'z'; ++i) {
			Element elt = new Element();
			elt.key = "key" + (char) i;
			int post = i % 4;
			if (post != 0)
				elt.post = "post" + post;
			elt.store();
		}

		assertEquals(10,
				StorageManagement.findElements().ofClass(Element.class)
						.withKey("key").between("keyd").and("keym").count());
	}

	@Test
	public void countConstrainedInTable() {
		for (int i = 'a'; i < 'z'; ++i) {
			Element elt = new Element();
			elt.key = "key" + (char) i;
			int post = i % 4;
			if (post != 0)
				elt.post = "post" + post;
			elt.store();
		}
		// 'd' is 100, so we should count 'd', 'h', 'l'
		assertEquals(3,
				StorageManagement.findElements().ofClass(Element.class)
						.withKey("key").between("keyd").and("keym").inTableWithPostfix("").count());
	}

	@Test
	public void findNone() {
		CloseableIterator<Element> res = StorageManagement.findElements()
				.ofClass(Element.class).withAtMost(1000).elements()
				.andActivateAllFamilies().iterate();
		assertFalse(res.hasNext());
		res.close();
	}

	@Test
	public void findAll() {
		for (int i = 0; i < 100; ++i) {
			Element elt = new Element();
			elt.key = "key" + i;
			int post = i % 4;
			if (post != 0)
				elt.post = "post" + post;
			elt.store();
		}

		CloseableIterator<Element> res = StorageManagement.findElements()
				.ofClass(Element.class).withAtMost(1000).elements()
				.andActivateAllFamilies().iterate();
		Element oldE = null;
		for (int i = 0; i < 100; ++i) {
			assertTrue(res.hasNext());
			Element newE = res.next();
			if (oldE != null)
				assertTrue(oldE.compareTo(newE) < 0);
			oldE = newE;
		}
		assertFalse(res.hasNext());
		res.close();
	}

	@Test
	public void findAllConstrained() {
		for (int i = 'a'; i < 'z'; ++i) {
			Element elt = new Element();
			elt.key = "key" + (char) i;
			int post = i % 4;
			if (post != 0)
				elt.post = "post" + post;
			elt.store();
		}

		NavigableSet<Element> res = StorageManagement.findElements()
				.ofClass(Element.class).withAtMost(1000).elements()
				.withKey("key").between("keyd").and("keym").go();
		assertEquals(10, res.size());
		Element elt = new Element();
		elt.key = "keyg";
		assertTrue(res.contains(elt));
		elt = new Element();
		elt.key = "keyc";
		assertFalse(res.contains(elt));
	}

	@Test
	public void findAllLimited() {
		for (int i = 'a'; i < 'z'; ++i) {
			Element elt = new Element();
			elt.key = "key" + (char) i;
			int post = i % 4;
			if (post != 0)
				elt.post = "post" + post;
			elt.store();
		}

		NavigableSet<Element> res = StorageManagement.findElements()
				.ofClass(Element.class).withAtMost(10).elements().go();
		assertEquals(10, res.size());
		Element elt = new Element();
		elt.key = "keyj";
		assertTrue(res.contains(elt));
		elt = new Element();
		elt.key = "keyk";
		assertFalse(res.contains(elt));
	}

	@Test
	public void tableSetAfterSearch() {
		for (int i = 0; i < 10; ++i) {
			Element elt = new Element();
			elt.key = "key" + i;
			int post = i % 4;
			if (post != 0)
				elt.post = "post" + post;
			elt.store();
		}

		CloseableIterator<Element> res = StorageManagement.findElements()
				.ofClass(Element.class).withAtMost(10).elements().iterate();
		try {
			assertTrue(res.hasNext());
			do {
				Element elt = res.next();
				int expectedIndex = Integer.parseInt(elt.key.substring("key"
						.length())) % 4;
				String expectedPostfix = expectedIndex == 0 ? "" : "post"
						+ expectedIndex;
				assertEquals("t" + expectedPostfix, elt.getActualTable());
			} while (res.hasNext());
		} finally {
			res.close();
		}
	}

	@Test
	public void findAllInTable() {
		for (int i = 0; i < 100; ++i) {
			Element elt = new Element();
			elt.key = "key" + i;
			int post = i % 4;
			if (post != 0)
				elt.post = "post" + post;
			elt.store();
		}

		CloseableIterator<Element> res = StorageManagement.findElements()
				.ofClass(Element.class).withAtMost(1000).elements()
				.inTableWithPostfix("post1").iterate();
		Element oldE = null;
		for (int i = 0; i < 25; ++i) {
			assertTrue(res.hasNext());
			Element newE = res.next();
			if (oldE != null)
				assertTrue(oldE.compareTo(newE) < 0);
			assertEquals("tpost1", newE.getActualTable());
			oldE = newE;
		}
		assertFalse(res.hasNext());
		res.close();
	}

	@Test
	public void findAllConstrainedInTable() {
		for (int i = 'a'; i < 'z'; ++i) {
			Element elt = new Element();
			elt.key = "key" + (char) i;
			int post = i % 4;
			if (post != 0)
				elt.post = "post" + post;
			elt.store();
		}

		NavigableSet<Element> res = StorageManagement.findElements()
				.ofClass(Element.class).withAtMost(1000).elements()
				.withKey("key").between("keyd").and("keym").inTableWithPostfix("post1").go();
		// 'd' is 100, so should be in table t
		// thus we have only 'e' and 'i' and 'm' in tpost1
		Element elt = new Element();
		elt.key = "keye";
		assertTrue(res.contains(elt));
		elt = new Element();
		elt.key = "keyi";
		assertTrue(res.contains(elt));
		elt = new Element();
		elt.key = "keym";
		assertTrue(res.contains(elt));
		assertEquals(3, res.size());
	}

	@Test(expected=IllegalArgumentException.class)
	public void settingTableInNonFederatedQuery() {
		StorageManagement.findElements()
				.ofClass(Book.class).withAtMost(1000).elements()
				.inTableWithPostfix("XXX");
	}

	@Test
	public void settingSameTwiceTableInQuery() {
		StorageManagement.findElements()
				.ofClass(Element.class).withAtMost(1000).elements()
				.inTableWithPostfix("post").inTableWithPostfix("post");
	}

	@Test(expected=IllegalArgumentException.class)
	public void settingDifferentTwiceTableInQuery() {
		StorageManagement.findElements()
				.ofClass(Element.class).withAtMost(1000).elements()
				.inTableWithPostfix("post1").inTableWithPostfix("post2");
	}
	
	@Test
	public void elementInTwoDifferentFederatedTables() throws Exception {
		Element e1 = new Element();
		e1.key = key;
		e1.post = "post";
		e1.arg = "element in table 1";
		e1.store();
		
		KeyManagement.getInstance().cleanupKnownPersistingElements();
		
		Element e2 = new Element();
		e2.key = key;
		e2.post = "post2";
		e2.arg = "element in table 2";
		e2.store();
		
		KeyManagement.getInstance().cleanupKnownPersistingElements();

		Element e = StorageManagement.findElements()
				.ofClass(Element.class)
				.withKey("key").setTo(key)
				.inTableWithPostfix("post")
				.andActivate()
				.any();
		assertEquals("element in table 1", e.arg);
		
		KeyManagement.getInstance().cleanupKnownPersistingElements();

		e = StorageManagement.findElements()
				.ofClass(Element.class)
				.withKey("key").setTo(key)
				.inTableWithPostfix("post2")
				.andActivate()
				.any();
		assertEquals("element in table 2", e.arg);
	}
	
	@Test(expected=DatabaseNotReachedException.class)
	public void readInconsistencyDetected() throws Exception {
		this.elementInTwoDifferentFederatedTables();
		
		KeyManagement.getInstance().cleanupKnownPersistingElements();

		StorageManagement.findElements()
				.ofClass(Element.class)
				.withKey("key").setTo(key)
				.withAtMost(10).elements()
				.go();
		// Element is read consistent
		// Data is both in table with postfixes post and post2
		// We should end up in read consistency issue
	}
	
	@Persisting(table = "t", federated = FederatedMode.RCONS)
	public static class MergableElement implements PersistingElementOverFederatedTableWithMerge {
		private static final long serialVersionUID = -6877770319760457398L;
		@Key
		public String key;
		public String post;
		public String arg;

		public String getTablePostfix() {
			return this.post;
		}
		
		public void mergeWith(PersistingElementOverFederatedTable elt) {
			MergableElement melt = (MergableElement)elt; 
			this.activateIfNotAlready();
			elt.activateIfNotAlready();
			if (this.arg == null || (melt.arg != null && melt.arg.compareTo(this.arg)>0))
				this.arg = melt.arg;
		}
	}
	
	@Test
	public void deleteInconsistentElement() throws Exception {
		this.elementInTwoDifferentFederatedTables();
		
		this.cleanupElement();
		
		assertEquals(0, StorageManagement.findElements()
				.ofClass(Element.class)
				.count());
	}
	
	@Test
	public void readInconsistencyDetectedAndRepaired() throws Exception {
		this.elementInTwoDifferentFederatedTables();
		
		KeyManagement.getInstance().cleanupKnownPersistingElements();

		NavigableSet<MergableElement> elts = StorageManagement.findElements()
				.ofClass(MergableElement.class)
				.withKey("key").setTo(key)
				.withAtMost(10).elements()
				.go();
		assertEquals(1, elts.size());
		MergableElement elt = elts.first();
		elt.activate();

		// Longest arg wins
		assertEquals("element in table 2", elt.arg);

		//Can't be sure of the element that is to be selected
		String selectedPost = elt.post;
		String otherPost = selectedPost.equals("post") ? "post2" : "post";
		
		// Element in table with other postfix is removed
		assertEquals(0, StorageManagement.findElements()
				.ofClass(MergableElement.class)
				.withKey("key").setTo(key)
				.inTableWithPostfix(otherPost)
				.andActivate()
				.count());
		
		KeyManagement.getInstance().unregister(elt);
		assertEquals(elt, StorageManagement.findElements()
				.ofClass(MergableElement.class)
				.withKey("key").setTo(key)
				.inTableWithPostfix(selectedPost)
				.andActivate()
				.any());
	}
	
	@Test
	public void readInconsistencyDetectedAndRepairedWithActivation() throws Exception {
		this.elementInTwoDifferentFederatedTables();
		
		KeyManagement.getInstance().cleanupKnownPersistingElements();

		NavigableSet<MergableElement> elts = StorageManagement.findElements()
				.ofClass(MergableElement.class)
				.withKey("key").setTo(key)
				.andActivate()
				.withAtMost(10).elements()
				.go();
		assertEquals(1, elts.size());
		MergableElement elt = elts.first();
		
		// Longest arg wins
		assertEquals("element in table 2", elt.arg);

		//Can't be sure of the element that is to be selected
		String selectedPost = elt.post;
		String otherPost = selectedPost.equals("post") ? "post2" : "post";
		
		// Element in table with other postfix is removed
		assertEquals(0, StorageManagement.findElements()
				.ofClass(MergableElement.class)
				.withKey("key").setTo(key)
				.inTableWithPostfix(otherPost)
				.andActivate()
				.count());
		
		KeyManagement.getInstance().unregister(elt);
		assertEquals(elt, StorageManagement.findElements()
				.ofClass(MergableElement.class)
				.withKey("key").setTo(key)
				.inTableWithPostfix(selectedPost)
				.andActivate()
				.any());
	}
	
	@Test
	public void readInconsistencyDetectedAndRepairedWithActivationInLargerList() throws Exception {
		int inPost = 0;
		int inPost2 = 0;
		
		for (char c = 'a' ; c < 'g' ; c++) {
			Element e = new Element();
			e.key = "key" + c;
			if (((int)c) % 2 == 0) {
				inPost++;
				e.post = "post";
			} else {
				inPost2++;
				e.post = "post2";
			}
			e.arg = Character.toString(c);
			e.store();
		}
		
		this.elementInTwoDifferentFederatedTables();
		
		for (char c = 'q' ; c < 'z' ; c++) {
			Element e = new Element();
			e.key = "key" + c;
			if (((int)c) % 2 == 0) {
				inPost++;
				e.post = "post";
			} else {
				inPost2++;
				e.post = "post2";
			}
			e.arg = Character.toString(c);
			e.store();
		}
		
		KeyManagement.getInstance().cleanupKnownPersistingElements();

		NavigableSet<MergableElement> elts = StorageManagement.findElements()
				.ofClass(MergableElement.class)
				.withKey("key").setTo(key)
				.andActivate()
				.withAtMost(10).elements()
				.go();
		assertEquals(1, elts.size());
		MergableElement elt = elts.first();
		
		// Longest arg wins
		assertEquals("element in table 2", elt.arg);

		//Can't be sure of the element that is to be selected
		String selectedPost = elt.post;
		String otherPost = selectedPost.equals("post") ? "post2" : "post";
		if (selectedPost.equals("post")) {
			inPost++;
		} else {
			inPost2++;
		}
		
		assertEquals(inPost, StorageManagement.findElements()
				.ofClass(MergableElement.class)
				.inTableWithPostfix("post")
				.andActivate()
				.count());
		
		assertEquals(inPost2, StorageManagement.findElements()
				.ofClass(MergableElement.class)
				.inTableWithPostfix("post2")
				.andActivate()
				.count());
		
		assertEquals(1, StorageManagement.findElements()
				.ofClass(MergableElement.class)
				.withKey("key").setTo(key)
				.inTableWithPostfix(selectedPost)
				.andActivate()
				.count());
		
		assertEquals(0, StorageManagement.findElements()
				.ofClass(MergableElement.class)
				.withKey("key").setTo(key)
				.inTableWithPostfix(otherPost)
				.andActivate()
				.count());
		
		assertEquals(inPost + inPost2, StorageManagement.findElements()
				.ofClass(MergableElement.class)
				.andActivate()
				.count());
		
		assertTrue(StorageManagement.findElements()
				.ofClass(MergableElement.class)
				.withKey("key").setTo(key)
				.andActivate()
				.withAtMost(1000).elements()
				.go()
				.contains(elt));
		
		KeyManagement.getInstance().unregister(elt);
	}
}
