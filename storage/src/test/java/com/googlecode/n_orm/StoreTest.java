package com.googlecode.n_orm;

import static org.junit.Assert.*;

import org.junit.Test;

import com.googlecode.n_orm.storeapi.SimpleStoreWrapper;
import com.googlecode.n_orm.storeapi.Store;
import com.googlecode.n_orm.memory.Memory;



public class StoreTest {
	@Test public void testNoPropertyFile() {
		com.googlecode.n_orm.nostoragefile.Element p = new com.googlecode.n_orm.nostoragefile.Element();
		Store s = p.getStore();
		assertSame(SimpleStoreWrapper.getWrapper(Memory.INSTANCE), s);
	}
	
	@Test(expected=IllegalStateException.class) public void testDummyPropertyFile() {
		com.googlecode.n_orm.dummystoragefile.Element p = new com.googlecode.n_orm.dummystoragefile.Element();
		p.getStore();
	}
	
	@Test public void testInstanciationPropertyFile() {
		com.googlecode.n_orm.simplestoragefile.Element p = new com.googlecode.n_orm.simplestoragefile.Element();
		Store s = p.getStore();
		assertSame(DummyStore.class, s.getClass());
		assertNotSame(DummyStore.INSTANCE, s);
		assertEquals("no id provided", ((DummyStore)s).getId());
		assertTrue(((DummyStore)s).isStarted());
	}
	
	@Test public void testKeyedInstanciationPropertyFile() {
		com.googlecode.n_orm.keyedstoragefile.Element p = new com.googlecode.n_orm.keyedstoragefile.Element();
		Store s = p.getStore();
		assertSame(DummyStore.class, s.getClass());
		assertNotSame(DummyStore.INSTANCE, s);
		assertEquals("keyed store given id", ((DummyStore)s).getId());
		assertTrue(((DummyStore)s).isStarted());
	}
	
	@Test public void testReferencedPropertyFile() {
		com.googlecode.n_orm.referencestoragefile.Element p = new com.googlecode.n_orm.referencestoragefile.Element();
		com.googlecode.n_orm.keyedstoragefile.Element q = new com.googlecode.n_orm.keyedstoragefile.Element();
		Store s = p.getStore();
		Store t = q.getStore();
		assertSame(DummyStore.class, s.getClass());
		assertSame(t, s);
		assertEquals("keyed store given id", ((DummyStore)s).getId());
		assertTrue(((DummyStore)s).isStarted());
	}
	
	@Test public void testDoubleReferencedPropertyFile() {
		com.googlecode.n_orm.doublereferencestoragefile.Element p = new com.googlecode.n_orm.doublereferencestoragefile.Element();
		com.googlecode.n_orm.keyedstoragefile.Element q = new com.googlecode.n_orm.keyedstoragefile.Element();
		Store s = p.getStore();
		Store t = q.getStore();
		assertSame(DummyStore.class, s.getClass());
		assertSame(t, s);
		assertEquals("keyed store given id", ((DummyStore)s).getId());
		assertTrue(((DummyStore)s).isStarted());
	}
	
	@Test public void testSingletonPropertyFile() {
		com.googlecode.n_orm.singletonstoragefile.Element p = new com.googlecode.n_orm.singletonstoragefile.Element();
		Store s = p.getStore();
		assertSame(DummyStore.class, s.getClass());
		assertSame(DummyStore.INSTANCE, s);
		assertTrue(((DummyStore)s).isStarted());
	}
	
	@Test public void testSameStore() {
		assertSame(new com.googlecode.n_orm.simplestoragefile.Element().getStore(), new com.googlecode.n_orm.simplestoragefile.Element2().getStore());
	}
}
