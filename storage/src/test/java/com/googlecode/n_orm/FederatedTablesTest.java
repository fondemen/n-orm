package com.googlecode.n_orm;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.cf.SetColumnFamily;
import com.googlecode.n_orm.memory.Memory;
import com.googlecode.n_orm.storeapi.SimpleStoreWrapper;

public class FederatedTablesTest {
	public static final String key = "akey";
	
	@Persisting(table="t",federated=FederatedMode.RCONS)
	public static class Element {
		@Key public String key;
		public String post;
		public String arg;
		public SetColumnFamily<String> cf = new SetColumnFamily<String>();
		
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
		//deleted by @Before method cleanupCache()
		
		assertTrue(elt.cf.isEmptyInStore());
	}
	
	@Test
	public void existsCfWhenCFDoesNotExists() {
		Element elt = new Element();
		elt.key = key;
		elt.store();
		
		Element elt2 = new Element();
		elt2.key = key;
		
		assertTrue(elt2.cf.isEmptyInStore());
	}
	
	@Test
	public void existsCfWhenCFExists() {
		Element elt = new Element();
		elt.key = key;
		elt.cf.add("AZERTY");
		elt.store();
		
		Element elt2 = new Element();
		elt2.key = key;
		
		assertFalse(elt2.cf.isEmptyInStore());
	}
}
