package com.googlecode.n_orm.cache;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.BookStore;
import com.googlecode.n_orm.StorageManagement;

public class CacheTest {
	
	static int originalCleanupPeriod;
	
	@BeforeClass
	public static void improveCacheCleanupPeriodicity() {
		originalCleanupPeriod = Cache.getPeriodBetweenCacheCleanupMS();
		Cache.setPeriodBetweenCacheCleanupMS(10);
	}
	
	@AfterClass
	public static void resetCacheCleanupPeriodicity() {
		Cache.setPeriodBetweenCacheCleanupMS(originalCleanupPeriod);
	}
	
	@Test
	public void  cacheOnceObjectCreated() {
		BookStore bs = new BookStore("testbookstore");
		bs.store();
		assertNotNull(Cache.findCache(Thread.currentThread()));
		assertSame(bs,Cache.getCache().getKnownPersistingElement(bs.getFullIdentifier()));
		assertSame(Cache.findCache(Thread.currentThread()), Cache.findCache(Thread.currentThread()));
	}

	@Test(timeout=11000)
	public void cacheCleanup() throws InterruptedException {
		int max = Cache.getMaxElementsInCache();
		Cache.setMaxElementsInCache(2);
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
		}
		
	}
	
	public static class WaitingThread extends Thread {
		public volatile Object waiter = new Object();
		private volatile Cache cache;
		private volatile boolean done = false;
		
		public Cache getCache() {
			while (cache == null)
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			return cache;
		}
		
		public void run() {
			cache = Cache.getCache();
			try {
				synchronized(this.waiter) {
					this.waiter.wait(10000);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			done = true;
		}
		
		public void end() {
			this.getCache();
			synchronized(this.waiter) {
				this.waiter.notify();
			}
			while (!done)
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}
	}
	
	@Test
	public void cacheForDifferentThreads() {
		WaitingThread t1 = new WaitingThread(), t2 = new WaitingThread();
		t1.start(); t2.start();
		try {
			Cache c1 = t1.getCache(), c2 = t2.getCache();
			assertNotSame(c1, c2);
		} finally {
			t1.end(); t2.end();
		}
	}
	
	@Test
	public void recyclingCacheForDifferentThreads() throws InterruptedException {
		Cache.cleanRecyclableCaches();
		Cache.waitNextCleanup();
		WaitingThread t1 = new WaitingThread(), t2 = new WaitingThread();
		t1.start();
		t1.end();
		assert !t1.isAlive();
		//Wait for next cache cleanup
		Cache.waitNextCleanup();
		t2.start();
		Cache c1 = t1.getCache(), c2 = t2.getCache();
		t2.end();
		assertSame(c1, c2);
	}
	
	@Test
	public void recyclingMostRecentCache() throws InterruptedException {
		Cache.cleanRecyclableCaches();
		Cache.waitNextCleanup();
		WaitingThread t1 = new WaitingThread(), t2 = new WaitingThread(), t3 = new WaitingThread();
		t1.start();
		t2.start();
		t1.end();
		Cache.waitNextCleanup();
		t2.end();
		Cache.waitNextCleanup();
		t3.start();
		Cache c2 = t2.getCache(), c3 = t3.getCache();
		t3.end();
		assertSame(c2, c3);
	}
	
	@Test
	public void cacheTooOld() throws InterruptedException {
		int initialTTL = Cache.getTimeToLiveSeconds();
		Cache.setTimeToLiveSeconds(0);
		try {
			Cache.cleanRecyclableCaches();
			Cache.waitNextCleanup();
			WaitingThread t1 = new WaitingThread(), t2 = new WaitingThread();
			t1.start();
			t1.end();
			assert !t1.isAlive();
			//Wait for next cache cleanup
			Cache.waitNextCleanup();
			Cache.waitNextCleanup();
			t2.start();
			Cache c1 = t1.getCache(), c2 = t2.getCache();
			t2.end();
			assertNotSame(c1, c2);
		} finally {
			Cache.setTimeToLiveSeconds(initialTTL);
		}
	}
}
