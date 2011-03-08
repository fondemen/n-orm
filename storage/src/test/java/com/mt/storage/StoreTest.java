package com.mt.storage;

import static org.junit.Assert.*;

import org.junit.Test;

import com.mt.storage.memory.Memory;


public class StoreTest {
	@Test public void testNoPropertyFile() {
		com.mt.storage.nostoragefile.Element p = new com.mt.storage.nostoragefile.Element();
		Store s = p.getStore();
		assertSame(Memory.INSTANCE, s);
	}
	
	@Test(expected=IllegalStateException.class) public void testDummyPropertyFile() {
		com.mt.storage.dummystoragefile.Element p = new com.mt.storage.dummystoragefile.Element();
		p.getStore();
	}
	
	@Test public void testInstanciationPropertyFile() {
		com.mt.storage.simplestoragefile.Element p = new com.mt.storage.simplestoragefile.Element();
		Store s = p.getStore();
		assertSame(DummyStore.class, s.getClass());
		assertNotSame(DummyStore.INSTANCE, s);
		assertEquals("no id provided", ((DummyStore)s).getId());
		assertTrue(((DummyStore)s).isStarted());
	}
	
	@Test public void testKeyedInstanciationPropertyFile() {
		com.mt.storage.keyedstoragefile.Element p = new com.mt.storage.keyedstoragefile.Element();
		Store s = p.getStore();
		assertSame(DummyStore.class, s.getClass());
		assertNotSame(DummyStore.INSTANCE, s);
		assertEquals("keyed store given id", ((DummyStore)s).getId());
		assertTrue(((DummyStore)s).isStarted());
	}
	
	@Test public void testSingletonPropertyFile() {
		com.mt.storage.singletonstoragefile.Element p = new com.mt.storage.singletonstoragefile.Element();
		Store s = p.getStore();
		assertSame(DummyStore.class, s.getClass());
		assertSame(DummyStore.INSTANCE, s);
		assertTrue(((DummyStore)s).isStarted());
	}
}
