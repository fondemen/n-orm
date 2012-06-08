package com.googlecode.n_orm;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

import java.util.TreeMap;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.googlecode.n_orm.SendingToStoreTest.Element;
import com.googlecode.n_orm.memory.Memory;
import com.googlecode.n_orm.storeapi.SimpleStoreWrapper;
import com.googlecode.n_orm.storeapi.Store;

public class FederatedTablesTest {
	
	@Persisting(table="t",federated=true)
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
	}

}
