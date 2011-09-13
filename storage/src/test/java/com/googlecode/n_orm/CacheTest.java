package com.googlecode.n_orm;

import static org.junit.Assert.*;

import org.junit.Test;

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
}
