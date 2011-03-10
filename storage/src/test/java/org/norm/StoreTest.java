package org.norm;

import static org.junit.Assert.*;

import org.junit.Test;
import org.norm.Store;
import org.norm.memory.Memory;



public class StoreTest {
	@Test public void testNoPropertyFile() {
		org.norm.nostoragefile.Element p = new org.norm.nostoragefile.Element();
		Store s = p.getStore();
		assertSame(Memory.INSTANCE, s);
	}
	
	@Test(expected=IllegalStateException.class) public void testDummyPropertyFile() {
		org.norm.dummystoragefile.Element p = new org.norm.dummystoragefile.Element();
		p.getStore();
	}
	
	@Test public void testInstanciationPropertyFile() {
		org.norm.simplestoragefile.Element p = new org.norm.simplestoragefile.Element();
		Store s = p.getStore();
		assertSame(DummyStore.class, s.getClass());
		assertNotSame(DummyStore.INSTANCE, s);
		assertEquals("no id provided", ((DummyStore)s).getId());
		assertTrue(((DummyStore)s).isStarted());
	}
	
	@Test public void testKeyedInstanciationPropertyFile() {
		org.norm.keyedstoragefile.Element p = new org.norm.keyedstoragefile.Element();
		Store s = p.getStore();
		assertSame(DummyStore.class, s.getClass());
		assertNotSame(DummyStore.INSTANCE, s);
		assertEquals("keyed store given id", ((DummyStore)s).getId());
		assertTrue(((DummyStore)s).isStarted());
	}
	
	@Test public void testSingletonPropertyFile() {
		org.norm.singletonstoragefile.Element p = new org.norm.singletonstoragefile.Element();
		Store s = p.getStore();
		assertSame(DummyStore.class, s.getClass());
		assertSame(DummyStore.INSTANCE, s);
		assertTrue(((DummyStore)s).isStarted());
	}
}
