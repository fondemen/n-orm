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
		Store store = Mockito.mock(Store.class);
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
	public void gettingFromTable() {
		Element elt = new Element();
		elt.key = "akey";
		elt.post = "post";
		elt.arg = "a value";
		elt.store();
		
		Element elt2 = new Element();
		elt2.key = "akey";
		elt2.post = "post";
		elt2.activate();
		
		assertEquals(elt.arg, elt2.arg);
	}

}
