package com.googlecode.n_orm;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.cf.MapColumnFamily;
import com.googlecode.n_orm.memory.Memory;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.SimpleStoreWrapper;

public class FederatedTablesTest {
	public static final String key = "akey";
	
	@Persisting(table="t",federated=FederatedMode.RCONS)
	public static class Element {
		@Key public String key;
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
				ok = false;
			}
		} while (!ok);
		
		//Mandatory as we are using different data stores for the same class in this test case
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

		assertNotNull(Memory.INSTANCE.getRow("tpost", elt.getIdentifier(), false));
		assertNull(Memory.INSTANCE.getRow("t", elt.getIdentifier(), false));
		assertEquals("tpost", elt.getTable());
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
		//A query to register the alternative table
		//Another one to actually store the element
		assertEquals(2, Memory.INSTANCE.getQueriesAndReset());
		
		FederatedTableManagement.clearAlternativesCache();
		
		Element elt2 = new Element();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = key;
		elt2.post = "post";
		elt2.activate();
		
		assertEquals(elt.arg, elt2.arg);
		//One query to register the alternative table
		//Another to find where the element is
		//And a last one to actually store the element
		assertEquals(3, Memory.INSTANCE.getQueriesAndReset());
		assertEquals("tpost", elt2.getTable());
	}
	
	@Test
	public void gettingFromKnownTable() {
		FederatedTableManagement.clearAlternativesCache();
		
		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post";
		elt.arg = "a value";
		elt.store(); //Caches the tpost table as an alternative to t

		Memory.INSTANCE.resetQueries(); //So that tpost table is forgotten in the store
		Element elt2 = new Element();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = key;
		elt2.activate(); //tpost table can only be found from cache
		
		assertEquals(elt.arg, elt2.arg);
		assertEquals("tpost", elt2.getTable());
	}
	
	@Test
	public void gettingFromUnKnownTable() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post";
		elt.arg = "a value";
		elt.store(); //Stores the tpost table as an alternative to t in stored metadata

		FederatedTableManagement.clearAlternativesCache(); //Forgets that tpost actually exists

		Element elt2 = new Element();
		elt2.key = key;
		elt2.activate(); //tpost table can only be found from meta data
		
		assertEquals(elt.arg, elt2.arg);
		assertEquals("tpost", elt2.getTable());
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
		assertEquals("tpost", elt2.getTable());
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
		assertEquals("tpost", elt2.getTable());
	}
	
	@Test
	public void alreadyKnownPostfixYetHasChanged() {
		Memory.INSTANCE.reset();
		
		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post1";
		elt.store();

		assertNotNull(Memory.INSTANCE.getRow("tpost1", elt.getIdentifier(), false));
		assertNull(Memory.INSTANCE.getRow("t", elt.getIdentifier(), false));
		assertNull(Memory.INSTANCE.getRow("tpost2", elt.getIdentifier(), false));
		assertEquals("tpost1", elt.getTable());
		
		elt.post = "post2";
		elt.store();

		assertNotNull(Memory.INSTANCE.getRow("tpost1", elt.getIdentifier(), false));
		assertNull(Memory.INSTANCE.getRow("t", elt.getIdentifier(), false));
		assertNull(Memory.INSTANCE.getRow("tpost2", elt.getIdentifier(), false));
		assertEquals("tpost1", elt.getTable());
	}
	
	@Persisting(table="t",federated=FederatedMode.PC_INCONS)
	public static class CheckedElement {
		@Key public String key;
		public String post;
		public String arg;
		
		public String getTablePostfix() {
			return this.post;
		}
	}
	
	@Test(expected=IllegalStateException.class)
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
	
	@Test(expected=IllegalStateException.class)
	public void alreadyKnownPostfixYetHasChangedButCheckedActivateVersion() {
		Memory.INSTANCE.reset();
		
		CheckedElement elt = new CheckedElement();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.post = "post1";
		elt.store(); //Stores key element in "tpost1"
		
		CheckedElement elt2 = new CheckedElement();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.post = "post2";
		elt2.activate(); //Finds key from "tpost1" while it should be found only from "tpost2" !
	}
	
	@Persisting(table="t",federated=FederatedMode.CONS)
	public static class ConsistentElement {
		@Key public String key;
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
		elt.store(); //Stores key element in "tpost1"
		
		FederatedTableManagement.clearAlternativesCache();
		
		ConsistentElement elt2 = new ConsistentElement();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = key;
		elt2.activate(); //Finds key from "tpost1" while it should be found only from "t" !

		assertEquals(elt.arg, elt2.arg);
		assertEquals(elt.post, elt2.post);
		assertEquals(elt.getTable(), elt2.getTable());
	}
	
	@Test
	public void alreadyKnownPostfixYetHasChangedButConsistentStoreVersion() {
		Memory.INSTANCE.reset();
		
		ConsistentElement elt = new ConsistentElement();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = key;
		elt.arg = "arg1";
		elt.post = "post1";
		elt.store(); //Stores key element in "tpost1"
		
		FederatedTableManagement.clearAlternativesCache();
		
		ConsistentElement elt2 = new ConsistentElement();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = key;
		elt2.arg = "arg2";
		elt2.post = "post2";
		elt2.store(); //Finds key from "tpost1" while it should be found only from "tpost2" !

		elt.activate();
		assertEquals("arg2", elt.arg);
		assertEquals("post2", elt.post);
		assertEquals("tpost1", elt2.getTable());
	}
	
	@Persisting(table="t",federated=FederatedMode.PC_LEG)
	public static class LegacyableElement {
		@Key public String key;
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
		elt.store(); //Stores key element in "tpost1" (legacy)
		
		FederatedTableManagement.clearAlternativesCache();
		
		LegacyableElement elt2 = new LegacyableElement();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = key;
		elt2.post = "post2";
		elt2.store(); //Misses "tpost1" !!!

		assertEquals("tpost2", elt2.getTable());

		//We are in an INCONSISTENT state !
		ConsistentElement celt = new ConsistentElement();
		celt.key = elt.key;
		assertTrue(celt.existsInStore());
		
		try {
			elt.delete();
		} catch (AssertionError x) {}
		assertTrue(elt.existsInStore()); //Found back from tpost2
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
	
	@Persisting(table="t")
	public static class SimpleElement {
		@Key public String key;
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
		//deleted by @Before method cleanupCache()
		
		assertTrue(elt.cf.isEmptyInStore());
		assertEquals("tpost1", elt.getTable());
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
		assertEquals("tpost1", elt2.getTable());
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
		assertEquals("tpost1", elt2.getTable());
	}
	
	@Test
	public void getCfWhenElementDoesNotExists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		//deleted by @Before method cleanupCache()
		
		assertNull(elt.cf.getFromStore("qual"));
		assertEquals("tpost1", elt.getTable());
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
		assertEquals("tpost1", elt2.getTable());
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
		
		assertEquals("tpost1", elt2.getTable());
	}
	
	@Test
	public void getAllCfWhenElementDoesNotExists() {
		Element elt = new Element();
		elt.key = key;
		elt.post = "post1";
		elt.cf.put("qual", "AZZERTYTEST");
		elt.cf.activate();
		//deleted by @Before method cleanupCache()
		
		assertEquals("tpost1", elt.getTable());

		assertTrue(elt.cf.isEmpty());

		//Testing again as table should already be known now and not go through aspect
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
		
		assertEquals("tpost1", elt2.getTable());

		assertTrue(elt2.cf.isEmpty());
		
		//Testing again as table should already be known now and not go through aspect
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
		
		assertEquals("tpost1", elt2.getTable());

		assertEquals("AZERTY", elt2.cf.get("qual"));

		//Testing again as table should already be known now and not go through aspect
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
		//deleted by @Before method cleanupCache()
		
		assertEquals("tpost1", elt.getTable());

		assertTrue(elt.cf.isEmpty());

		//Testing again as table should already be known now and not go through aspect
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
		
		assertEquals("tpost1", elt2.getTable());

		assertTrue(elt2.cf.isEmpty());
		
		//Testing again as table should already be known now and not go through aspect
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
		
		assertEquals("tpost1", elt2.getTable());

		assertEquals("AZERTY2", elt2.cf.get("QUAL"));
		assertNull(elt2.cf.get("qual"));

		//Testing again as table should already be known now and not go through aspect
		elt.cf.activate(new Constraint("A", "Z"));
		assertEquals("AZERTY2", elt.cf.get("QUAL"));
		assertNull(elt.cf.get("qual"));
		elt2.cf.activate(new Constraint("A", "Z"));
		assertEquals("AZERTY2", elt2.cf.get("QUAL"));
		assertNull(elt2.cf.get("qual"));
	}
	
	@Test
	public void countNone() {
		assertEquals(0, StorageManagement.findElements().ofClass(Element.class).count());
	}
	
	@Test
	public void countAll() {
		for(int i = 0; i < 100; ++i) {
			Element elt = new Element();
			elt.key = "key" + i;
			int post  = i%4;
			if (post != 0)
				elt.post = "post" + post;
			elt.store();
		}
		
		assertEquals(100, StorageManagement.findElements().ofClass(Element.class).count());
	}
	
	@Test
	public void countConstrained() {
		for(int i = 'a'; i < 'z'; ++i) {
			Element elt = new Element();
			elt.key = "key" + (char)i;
			int post  = i%4;
			if (post != 0)
				elt.post = "post" + post;
			elt.store();
		}
		
		assertEquals(10, StorageManagement.findElements().ofClass(Element.class).withKey("key").between("keyd").and("keym").count());
	}
}
