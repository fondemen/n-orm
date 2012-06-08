package com.googlecode.n_orm;

import static org.junit.Assert.*;

import org.junit.Test;

import com.googlecode.n_orm.Persisting.FederatedMode;
import com.googlecode.n_orm.memory.Memory;
import com.googlecode.n_orm.storeapi.SimpleStoreWrapper;

public class FederatedTablesTest {
	
	@Persisting(table="t",federated=FederatedMode.FAST_UNCHECKED)
	public static class Element {
		@Key public String key;
		public String post;
		public String arg;
		
		public String getTablePostfix() {
			return this.post;
		}
	}
	
	@Test
	public void sendingToTable() {
		Memory.INSTANCE.reset();
		
		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = "akey";
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
		elt.key = "akey";
		elt.post = "post";
		elt.arg = "a value";
		elt.store();
		//One query to register the alternative table
		//Another to actually store the element
		assertEquals(2, Memory.INSTANCE.getQueriesAndReset());
		
		Element elt2 = new Element();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = "akey";
		elt2.post = "post";
		elt2.activate();
		
		assertEquals(elt.arg, elt2.arg);
		assertEquals(1, Memory.INSTANCE.getQueriesAndReset());
		assertEquals("tpost", elt2.getTable());
	}
	
	@Test
	public void gettingFromKnownTable() {
		FederatedTableManagement.clearAlternativesCache();
		
		Element elt = new Element();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = "akey";
		elt.post = "post";
		elt.arg = "a value";
		elt.store(); //Caches the tpost table as an alternative to t

		Memory.INSTANCE.resetQueries(); //So that tpost table is forgotten in the store
		Element elt2 = new Element();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = "akey";
		elt2.activate(); //tpost table can only be found from cache
		
		assertEquals(elt.arg, elt2.arg);
		assertEquals("tpost", elt2.getTable());
	}
	
	@Test
	public void gettingFromUnKnownTable() {
		Element elt = new Element();
		elt.key = "akey";
		elt.post = "post";
		elt.arg = "a value";
		elt.store(); //Stores the tpost table as an alternative to t in stored metadata

		FederatedTableManagement.clearAlternativesCache(); //Forgets that tpost actually exists

		Element elt2 = new Element();
		elt2.key = "akey";
		elt2.activate(); //tpost table can only be found from meta data
		
		assertEquals(elt.arg, elt2.arg);
		assertEquals("tpost", elt2.getTable());
	}
	
	@Test
	public void storeAndActivate() {
		Element elt = new Element();
		elt.key = "akey";
		elt.post = "post";
		elt.arg = "a value";
		elt.store();
		
		Element elt2 = new Element();
		elt2.key = "akey";
		elt2.activate();

		assertEquals(elt.post, elt2.post);
		assertEquals(elt.arg, elt2.arg);
		assertEquals("tpost", elt2.getTable());
	}
	
	@Test
	public void storeAndActivateIfNotAlready() {
		Element elt = new Element();
		elt.key = "akey";
		elt.post = "post";
		elt.arg = "a value";
		elt.store();
		
		Element elt2 = new Element();
		elt2.key = "akey";
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
		elt.key = "akey";
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
	
	@Persisting(table="t",federated=FederatedMode.FAST_CHECKED)
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
		elt.key = "akey";
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
		elt.key = "akey";
		elt.post = "post1";
		elt.store(); //Stores "akey" element in "tpost1"
		
		CheckedElement elt2 = new CheckedElement();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.post = "post2";
		elt2.activate(); //Finds "akey" from "tpost1" while it should be found only from "tpost2" !
	}
	
	@Persisting(table="t",federated=FederatedMode.CONSISTENT)
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
		elt.key = "akey";
		elt.post = "post1";
		elt.store(); //Stores "akey" element in "tpost1"
		
		ConsistentElement elt2 = new ConsistentElement();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = "akey";
		elt2.activate(); //Finds "akey" from "tpost1" while it should be found only from "t" !

		assertEquals(elt.arg, elt2.arg);
		assertEquals(elt.post, elt2.post);
		assertEquals(elt.getTable(), elt2.getTable());
	}
	
	@Test
	public void alreadyKnownPostfixYetHasChangedButConsistentStoreVersion() {
		Memory.INSTANCE.reset();
		
		ConsistentElement elt = new ConsistentElement();
		elt.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt.key = "akey";
		elt.arg = "arg1";
		elt.post = "post1";
		elt.store(); //Stores "akey" element in "tpost1"
		
		ConsistentElement elt2 = new ConsistentElement();
		elt2.setStore(SimpleStoreWrapper.getWrapper(Memory.INSTANCE));
		elt2.key = "akey";
		elt2.arg = "arg2";
		elt2.post = "post2";
		elt2.store(); //Finds "akey" from "tpost1" while it should be found only from "tpost2" !

		elt.activate();
		assertEquals("arg2", elt.arg);
		assertEquals("post2", elt.post);
		assertEquals("tpost1", elt2.getTable());
	}
	
	@Test
	public void exists() {
		Element elt = new Element();
		elt.key = "akey";
		elt.post = "post";
		
		assertFalse(elt.existsInStore());
		
		elt.store();
		
		Element elt2 = new Element();
		elt2.key = "akey";

		assertTrue(elt2.existsInStore());
		assertTrue(elt.existsInStore());
	}

}
