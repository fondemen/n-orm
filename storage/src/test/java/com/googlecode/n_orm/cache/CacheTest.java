package com.googlecode.n_orm.cache;

import static org.junit.Assert.*;

import org.junit.Test;

import com.googlecode.n_orm.BookStore;
import com.googlecode.n_orm.StorageManagement;

public class CacheTest {
	
	@Test
	public void  cacheOnceObjectCreated() {
		BookStore bs = new BookStore("testbookstore");
		bs.store();
		assertNotNull(Cache.findCache(Thread.currentThread()));
		assertSame(bs,Cache.getCache().getKnownPersistingElement(bs.getFullIdentifier()));
	}

	@Test
	public void cacheCleanup() throws InterruptedException {
		int max = Cache.getMaxElementsInCache();
		Cache.setMaxElementsInCache(2);
		int cleanupPeriod = Cache.getPeriodBetweenCacheCleanupSeconds();
		Cache.setPeriodBetweenCacheCleanupSeconds(1);
		try {
			final AssertionError[] error = new AssertionError [1];
			final BookStore bs = new BookStore("testbookstore");
			
			Runnable runnable = new Runnable() {
				
				@Override
				public void run() {
					try {
						Thread t = Thread.currentThread();
						assertFalse(Cache.knowsCache(t));
						assertNull(Cache.findCache(t));
						StorageManagement.getElement(BookStore.class, bs.getIdentifier());
						Cache c = Cache.findCache(t);
						assertNotNull(c);
						assertTrue(Cache.knowsCache(t));
						assertEquals(1, c.size());
						assertNotNull(Cache.getCache().getKnownPersistingElement(bs.getFullIdentifier()));
						
						BookStore bs2 = new BookStore("bs2");
						bs2.activate();
						assertEquals(2, c.size());
						assertNotNull(Cache.getCache().getKnownPersistingElement(bs.getFullIdentifier()));
						assertNotNull(Cache.getCache().getKnownPersistingElement(bs2.getFullIdentifier()));
						
						BookStore bs3 = new BookStore("bs3");
						bs3.activate();
						assertEquals(2, c.size());
						assertNull(Cache.getCache().getKnownPersistingElement(bs.getFullIdentifier()));
						assertNotNull(Cache.getCache().getKnownPersistingElement(bs2.getFullIdentifier()));
						assertNotNull(Cache.getCache().getKnownPersistingElement(bs2.getFullIdentifier()));
						
					} catch (AssertionError r) {
						error[0] = r;
					}
				}
			};
			final Thread t = new Thread(runnable);
			t.start();
			do {
				Thread.sleep(500);
				if (error[0] != null)
					throw error[0];
			} while (t.isAlive());
			
			Cache.waitNextCleanup();

			assertNull(Cache.findCache(t));
			assertFalse(Cache.knowsCache(t));
		} finally {
			Cache.setMaxElementsInCache(max);
			Cache.setPeriodBetweenCacheCleanupSeconds(cleanupPeriod);
		}
		
	}
}
