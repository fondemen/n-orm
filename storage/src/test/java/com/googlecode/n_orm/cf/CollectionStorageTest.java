package com.googlecode.n_orm.cf;

import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.AddOnly;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.ImplicitActivation;
import com.googlecode.n_orm.Incrementing;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.MemoryStoreTestLauncher;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.StoreTestLauncher;



public class CollectionStorageTest {
	private MemoryStoreTestLauncher mstl;
	
	public CollectionStorageTest() throws Exception {
		StoreTestLauncher stl = StoreTestLauncher.INSTANCE;
		Properties props = stl.prepare(this.getClass());
		if (stl instanceof MemoryStoreTestLauncher)
			this.mstl = (MemoryStoreTestLauncher)stl;
		StoreTestLauncher.registerStorePropertiesForInnerClasses(getClass());
	}
	
	private void assertHadAQuery() {
		if (this.mstl != null)
			this.mstl.assertHadAQuery();
	}
	
	private void assertHadNoQuery() {
		if (this.mstl != null)
			this.mstl.assertHadNoQuery();
	}

	private void resetQueryCount() {
		if (this.mstl != null)
			this.mstl.resetQueryCount();
	}
	
	public static class Element {
		@Key public int anint;
		@Key(order=2) public String key;
		
		public Element(){}

		public Element(int anint, String key) {
			this.anint = anint;
			this.key = key;
		}

		public Element(String key) {
			this(0, key);
		}

		@Override
		public boolean equals(Object rhs) {
			if (rhs == null)
				return false;
			
			if (this == rhs)
				return true;
			
			if (!(rhs instanceof Element))
				return false;
			
			return this.key.equals(((Element)rhs).key);
		}

		@Override
		public int hashCode() {
			return this.key.hashCode();
		}
		
	}

	@Persisting(table="TestContainer")
	public static class Container {
		private static final long serialVersionUID = 6300245095742806502L;
		@Key public String key;
		@ImplicitActivation public Set<Element> elements = new HashSet<CollectionStorageTest.Element>();
		@Incrementing public Map<String, Integer> elementsInc = new HashMap<String, Integer>();
		public long prop;
		
		public Container() {}

		public Container(String key) {
			this.key = key;
		}
	}
	
	public Container sut;
	
	@Before
	public void setupSut() throws DatabaseNotReachedException {
		this.resetQueryCount();
		sut = new Container("key");
		assertTrue(sut.getColumnFamily(sut.elements).isEmptyInStore());
		this.assertHadAQuery();
		assertTrue(sut.getColumnFamily(sut.elements).isEmptyInStore());
		this.assertHadAQuery();
		sut.activate("elements");
		this.assertHadAQuery();
		for(int i = 1 ; i <= 10; ++i) {
			sut.elements.add(new Element("E" + i));
			sut.elementsInc.put("E" + i, i);
		}
		sut.store();
		this.assertHadAQuery();
		assertFalse(sut.getColumnFamily(sut.elements).isEmptyInStore());
		this.assertHadAQuery();
		assertFalse(sut.getColumnFamily(sut.elements).isEmptyInStore());
		this.assertHadAQuery();
		KeyManagement.getInstance().cleanupKnownPersistingElements(); //Simulates a new session
	}

	@After public void deleteSut() throws DatabaseNotReachedException {
		sut.delete();
		assertFalse(sut.existsInStore());
		assertTrue(sut.getColumnFamily(sut.elements).isEmptyInStore());
		assertTrue(sut.getColumnFamily(sut.elements).isEmptyInStore());
	}
	
	@Test
	public void hasFamily() {
		assertEquals(SetColumnFamily.class, sut.getColumnFamily(sut.elements).getClass());
		assertEquals(MapColumnFamily.class, sut.getColumnFamily(sut.elementsInc).getClass());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void autoActivation() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		copy.activate();
		assertHadAQuery();
		assertTrue(((ColumnFamily<Element>)copy.getColumnFamily("elements")).isActivated());
	}
	
	@Test
	public void ifNotAlready() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		copy.activateIfNotAlready();
		assertHadAQuery();
		copy.activateIfNotAlready();
		assertHadNoQuery();
		copy.activateIfNotAlready();
		assertHadNoQuery();
	}
	
	@Test
	public void ifNotAlreadyTimeouted() throws DatabaseNotReachedException, InterruptedException {
		Container copy = new Container(sut.key);
		copy.activateIfNotAlready(99);
		assertHadAQuery();
		copy.activateIfNotAlready(99);
		assertHadNoQuery();
		Thread.sleep(100);
		copy.activateIfNotAlready(99);
		assertHadAQuery();
	}
	
	@Test
	public void storeRetrieveElements() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		copy.activate("elements");
		this.assertHadAQuery();
		assertEquals(sut.elements.size(), copy.elements.size());
		this.assertHadNoQuery();
		for(int i = 1; i <= sut.elements.size(); ++i) {
			assertTrue(copy.elements.contains(new Element("E" + i)));
		}
		assertTrue(copy.elements.containsAll(sut.elements));
		this.assertHadNoQuery();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void storeRetrieveElementsFrom3To65() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		((ColumnFamily<Element>)copy.getColumnFamily("elements")).activate(new Element("E3"), new Element("E65"));
		this.assertHadAQuery();
		assertEquals(4, copy.elements.size());
		this.assertHadNoQuery();
		for(int i = 3; i <= 6; ++i) {
			assertTrue(copy.elements.contains(new Element("E" + i)));
		}
		this.assertHadNoQuery();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void storeRetrieveElementsFrom7ToEnd() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		((ColumnFamily<Element>)copy.getColumnFamily(copy.elements)).activate(new Element("E7"), null);
		this.assertHadAQuery();
		assertEquals(3, copy.elements.size());
		this.assertHadNoQuery();
		for(int i = 7; i <= 9; ++i) {
			assertTrue(copy.elements.contains(new Element("E" + i)));
		}
		this.assertHadNoQuery();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void storeRetrieveElementsUpTo4() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		((ColumnFamily<Element>)copy.getColumnFamily("elements")).activate(null, new Element("E4"));
		this.assertHadAQuery();
		assertEquals(5, copy.elements.size());
		this.assertHadNoQuery();
		for(int i = 1; i <= 4; ++i) {
			assertTrue(copy.elements.contains(new Element("E" + i)));
		}
		assertTrue(copy.elements.contains(new Element("E10")));
		this.assertHadNoQuery();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void change() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		copy.activate("elements");
		this.assertHadAQuery();
		assertTrue(copy.elements.remove(new Element("E4")));
		copy.updateFromPOJO();
		assertTrue(((ColumnFamily<Element>)copy.getColumnFamily("elements")).wasDeleted(KeyManagement.getInstance().createIdentifier(new Element("E4"), null)));
		this.assertHadNoQuery();
	}

	
	@Persisting(table="TestContainer")
	public static class AddonlyContainer {
		private static final long serialVersionUID = -8766880305630431989L;
		@Key public String key;
		@AddOnly public Set<Element> elements = null;

		public AddonlyContainer(String key) {
			this.key = key;
		}
	}
	@Test
	public void unauthorizedRemoveNoStore() throws DatabaseNotReachedException {
		AddonlyContainer copy = new AddonlyContainer(sut.key);
		copy.activate("elements");
		this.assertHadAQuery();
		assertEquals(sut.elements.size(), copy.elements.size());
		assertTrue(copy.elements.remove(new Element("E4")));
		assertFalse(copy.elements.contains(new Element("E4")));
		this.assertHadNoQuery();
	}
	@Test(expected=IllegalStateException.class)
	public void unauthorizedRemoveStore() throws DatabaseNotReachedException {
		AddonlyContainer copy = new AddonlyContainer(sut.key);
		copy.activate("elements");
		this.assertHadAQuery();
		assertEquals(sut.elements.size(), copy.elements.size());
		assertTrue(copy.elements.remove(new Element("E4")));
		this.assertHadNoQuery();
		copy.store();
		this.assertHadNoQuery();
	}
	
	@Test
	public void activatingNullProperty() throws DatabaseNotReachedException {
		AddonlyContainer copy = new AddonlyContainer(sut.key);
		copy.activate("elements");
		this.assertHadAQuery();
		assertEquals(sut.elements.size(), copy.elements.size());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void containsElementFromStore() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		this.assertHadNoQuery();
		assertEquals(0, copy.elements.size());
		this.assertHadNoQuery();
		assertTrue(!copy.elements.contains(new Element("GFYUBY")));
		this.assertHadNoQuery();
		assertFalse(((SetColumnFamily<?>)copy.getColumnFamily(copy.elements)).containsInStore(new Element("GFYUBY")));
		assertTrue(copy.elements.isEmpty());
		this.assertHadAQuery();
		assertFalse(copy.elements.contains(new Element("E6")));
		this.assertHadNoQuery();
		assertTrue(((SetColumnFamily<Element>)copy.getColumnFamily(copy.elements)).containsInStore(new Element("E6")));
		assertEquals(1, copy.elements.size());
		this.assertHadAQuery();
		assertTrue(copy.elements.contains(new Element("E6")));
		this.assertHadNoQuery();
	}
	
	@Test
	public void newElement() throws DatabaseNotReachedException {
		Container copy = new Container("other-key");
		this.assertHadNoQuery();
		assertTrue(sut.existsInStore());
		this.assertHadAQuery();
		assertTrue(!copy.existsInStore());
		this.assertHadAQuery();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void increment() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		copy.activate("elementsInc");
		this.assertHadAQuery();
		copy.elementsInc.put("E4", 9);
		copy.updateFromPOJO();
		assertFalse(((ColumnFamily<Element>)copy.getColumnFamily("elementsInc")).wasDeleted("E4"));
		assertTrue(((ColumnFamily<Element>)copy.getColumnFamily("elementsInc")).wasChanged("E4"));
		assertEquals(5, ((ColumnFamily<Element>)copy.getColumnFamily("elementsInc")).getIncrement("E4"));
		this.assertHadNoQuery();
		copy.store();
		copy = new Container(sut.key);
		copy.activate("elementsInc");
		assertEquals(9, (int)copy.elementsInc.get("E4"));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void legalActivityCheck() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		copy.activate("elements");
		((ColumnFamily<Element>)copy.getColumnFamily("elements")).assertIsActivated(" testing legalActivityCheck");
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void illegalActivityCheck() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		copy.activate("elements");
		((ColumnFamily<Element>)copy.getColumnFamily("elements")).assertIsActivated("testing illegalActivityCheck");
	}
	
	@Test
	public void unknownActivated() throws DatabaseNotReachedException {
		Container copy = new Container("gyujgynugyiul,huohuo,");
		copy.activate("elements", "elementsInc");
		assertTrue(((ColumnFamily<?>)copy.getColumnFamily("elements")).isActivated());
		assertTrue(((ColumnFamily<?>)copy.getColumnFamily("elementsInc")).isActivated());
	}
	
	@Test
	public void multipleActivations() {
		sut.activateColumnFamily("elements");
		this.assertHadAQuery();
		sut.activateColumnFamilyIfNotAlready("elements");
		this.assertHadNoQuery();
		sut.activateColumnFamilyIfNotAlready("elements");
		this.assertHadNoQuery();
		sut.activateColumnFamily("elements");
		this.assertHadAQuery();
	}
	
	@Test
	public void multipleActivationsTimeouted() throws InterruptedException {
		sut.activateColumnFamily("elements");
		this.assertHadAQuery();
		Thread.sleep(100);
		sut.activateColumnFamilyIfNotAlready("elements", 99);
		this.assertHadAQuery();
		sut.activateColumnFamilyIfNotAlready("elements", 99);
		this.assertHadNoQuery();
		sut.activateColumnFamily("elements");
		this.assertHadAQuery();
	}
	
	@Test
	public void activateWithPropChanged() {
		sut.prop = 12;
		sut.store();
		Container sut2 = new Container(); sut2.key = sut.key;
		sut2.activate();
		assertEquals(12, sut2.prop);
		sut2.prop = 123;
		sut2.activateIfNotAlready("elementsInc");
		
		assertEquals(123, sut2.prop);
	}
	
	@Test
	public void activateIfNotAlreadyIfAlreadyAndChanged() {
		sut.activate("elements");
		
		Element e = new Element(-1, "test");
		sut.elements.add(e);
		
		assertTrue(sut.elements.contains(e));
		
		KeyManagement.getInstance().cleanupKnownPersistingElements();
		
		sut.activateIfNotAlready("elements");
		
		assertTrue(sut.elements.contains(e));
	}
	
	@Test(expected=ConcurrentModificationException.class, timeout=5000)
	public void concurrentChangeAndUpdateUnsynchronized() throws Exception {
		sut.elements.add(new Element(456, "dummy"));
		final boolean [] done = {false};
		final Exception [] failed = {null};
		final Thread t2 = new Thread(new Runnable() {
			
			@Override
			public void run() {
				int i = 4536;
				Calendar d = Calendar.getInstance();
				d.add(Calendar.MILLISECOND, 3000);
				while (!done[0] && Calendar.getInstance().before(d)) {
					i++;
					sut.elements.add(new Element(i, "dummy"));
					i++;
					sut.elements.add(new Element(i, "dummy"));
					sut.elements.remove(new Element(i-1, "dummy"));
				}
			}
		});
		t2.setPriority(Thread.MAX_PRIORITY);
		Thread t1 = new Thread(new Runnable() {
			
			@Override
			public void run() {
				t2.start();
				try {
					Thread.sleep(100);
					sut.updateFromPOJO();
				} catch(Exception x) {
					failed[0] = x;
				}
				
				done[0] = true;
			}
		});
		t1.setPriority(Thread.MIN_PRIORITY);
		((SetColumnFamily<?>)sut.getColumnFamily(sut.elements)).goSlow();
		t1.start();
		while(!done[0])
			Thread.sleep(100);
		if (failed[0] != null)
			throw failed[0];
	}
	
	@Test(expected=Test.None.class, timeout=5000)
	public void concurrentChangeAndUpdateSynchronizedOnSut() throws Exception {
		sut.elements.add(new Element(456, "dummy"));
		final boolean [] done = {false};
		final Exception [] failed = {null};
		final Thread t2 = new Thread(new Runnable() {
			
			@Override
			public void run() {
				int i = 4536;
				while (!done[0]) {
					synchronized(sut) { //would actually solve the problem
					i++;
					sut.elements.add(new Element(i, "dummy"));
					i++;
					sut.elements.add(new Element(i, "dummy"));
					sut.elements.remove(new Element(i-1, "dummy"));
					}
				}
			}
		});
		t2.setPriority(Thread.MAX_PRIORITY);
		Thread t1 = new Thread(new Runnable() {
			
			@Override
			public void run() {
				t2.start();
				try {
					Thread.sleep(100);
					sut.updateFromPOJO();
				} catch(Exception x) {
					failed[0] = x;
				}
				
				done[0] = true;
			}
		});
		t1.setPriority(Thread.MIN_PRIORITY);
		((SetColumnFamily<?>)sut.getColumnFamily(sut.elements)).goSlow();
		t1.start();
		while(!done[0])
			Thread.sleep(100);
		if (failed[0] != null)
			throw failed[0];
	}
	
	@Test(expected=Test.None.class, timeout=5000)
	public void concurrentChangeAndUpdateSynchronizedOnCF() throws Exception {
		sut.elements.add(new Element(456, "dummy"));
		final boolean [] done = {false};
		final Exception [] failed = {null};
		final Thread t2 = new Thread(new Runnable() {
			
			@Override
			public void run() {
				int i = 4536;
				while (!done[0]) {
					synchronized(sut.elements) { //would actually solve the problem
					i++;
					sut.elements.add(new Element(i, "dummy"));
					i++;
					sut.elements.add(new Element(i, "dummy"));
					sut.elements.remove(new Element(i-1, "dummy"));
					}
				}
			}
		});
		t2.setPriority(Thread.MAX_PRIORITY);
		Thread t1 = new Thread(new Runnable() {
			
			@Override
			public void run() {
				t2.start();
				try {
					Thread.sleep(100);
					sut.updateFromPOJO();
				} catch(Exception x) {
					failed[0] = x;
				}
				
				done[0] = true;
			}
		});
		t1.setPriority(Thread.MIN_PRIORITY);
		((SetColumnFamily<?>)sut.getColumnFamily(sut.elements)).goSlow();
		t1.start();
		while(!done[0])
			Thread.sleep(100);
		if (failed[0] != null)
			throw failed[0];
	}
	
	@Test(expected=Test.None.class, timeout=5000)
	public void concurrentChangeAndUpdateFromSearchOnCF() throws Exception {
		sut.store();
		sut.elements.add(new Element(456, "dummy"));
		final boolean [] done = {false};
		final Exception [] failed = {null};
		final Thread t2 = new Thread(new Runnable() {
			
			@Override
			public void run() {
				Container sutT = StorageManagement.findElements().ofClass(Container.class).andActivate("elements").withId(sut.getIdentifier());
				assertNotSame(sut, sutT);
				((SetColumnFamily<?>)sutT.getColumnFamily(sutT.elements)).goSlow();
				int i = 4536;
				while (!done[0]) {
					i++;
					sutT.elements.add(new Element(i, "dummy"));
					i++;
					sutT.elements.add(new Element(i, "dummy"));
					sutT.elements.remove(new Element(i-1, "dummy"));
				}
			}
		});
		t2.setPriority(Thread.MAX_PRIORITY);
		Thread t1 = new Thread(new Runnable() {
			
			@Override
			public void run() {
				Container sutT = StorageManagement.findElements().ofClass(Container.class).andActivate("elements").withId(sut.getIdentifier());
				assertNotSame(sut, sutT);
				((SetColumnFamily<?>)sutT.getColumnFamily(sutT.elements)).goSlow();
				t2.start();
				try {
					Thread.sleep(100);
					sutT.updateFromPOJO();
				} catch(Exception x) {
					failed[0] = x;
				}
				
				done[0] = true;
			}
		});
		t1.setPriority(Thread.MIN_PRIORITY);
		((SetColumnFamily<?>)sut.getColumnFamily(sut.elements)).goSlow();
		t1.start();
		while(!done[0])
			Thread.sleep(100);
		if (failed[0] != null)
			throw failed[0];
	}
}
