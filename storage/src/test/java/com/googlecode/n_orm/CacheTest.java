package com.googlecode.n_orm;

import static org.junit.Assert.*;

import org.aspectj.lang.annotation.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class CacheTest {

	@Persisting
	public static class Element {
		@Key private int key1;
		@Key(order=2) private String key2;
		public Element(int key1, String key2) {
			super();
			this.key1 = key1;
			this.key2 = key2;
		}
	}
	
	@org.junit.Before public void vacuumCache() {
		KeyManagement.getInstance().cleanupKnownPersistingElements();
	}
	
	@Test public void elementInCache() {
		Element elt1 = new Element(12, "huifhsu");
		assertSame(elt1, StorageManagement.getElementUsingCache(elt1));
		Element elt2 = new Element(12, "huifhsu");
		assertEquals(elt1, elt2);
		assertNotSame(elt1, elt2);
		assertSame(elt1, StorageManagement.getElementUsingCache(elt1));
		assertSame(elt1, StorageManagement.getElementUsingCache(elt2));
		assertSame(elt1, elt1.getCachedVersion());
		assertSame(elt1, elt2.getCachedVersion());
	}
	
	@Test public void elementInVacuumedCache() {
		Element elt1 = new Element(12, "huifhsu");
		assertSame(elt1, StorageManagement.getElementUsingCache(elt1));
		KeyManagement.getInstance().cleanupKnownPersistingElements();
		Element elt2 = new Element(12, "huifhsu");
		assertEquals(elt1, elt2);
		assertNotSame(elt1, elt2);
		assertSame(elt2, StorageManagement.getElementUsingCache(elt2));
		assertSame(elt2, StorageManagement.getElementUsingCache(elt1));
		assertSame(elt2, elt1.getCachedVersion());
		assertSame(elt2, elt2.getCachedVersion());
		assertSame(elt2, StorageManagement.getElementUsingCache(elt1));
		assertSame(elt2, StorageManagement.getElementUsingCache(elt2));
		assertSame(elt2, elt1.getCachedVersion());
		assertSame(elt2, elt2.getCachedVersion());
	}
	
	@Test public void elementFromKeys() {
		Element elt1 = StorageManagement.getElementWithKeys(Element.class, 12, "huifhsu");
		Element elt2 = StorageManagement.getElementWithKeys(Element.class, 12, "huifhsu");
		assertSame(elt1, elt2);
		Element elt3 = StorageManagement.getElementWithKeys(Element.class, 12, "huiwfhsu");
		assertNotSame(elt1, elt3);
	}
}
