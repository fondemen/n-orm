package com.mt.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mt.storage.cf.CollectionColumnFamily;
import com.mt.storage.cf.ColumnFamily;
import com.mt.storage.cf.MapColumnFamily;


public class CollectionStorageTest {
	private MemoryStoreTestLauncher mstl;
	
	public CollectionStorageTest() throws Exception {
		StoreTestLauncher stl = StoreTestLauncher.INSTANCE;
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
		@Key public final int anint;
		@Key(order=2) public final String key;

		public Element(int anint, String key) {
			this.anint = anint;
			this.key = key;
		}

		public Element(String key) {
			this(0, key);
		}
		
	}

	@Persisting(table="TestContainer")
	public static class Container {
		@Key public final String key;
		@Indexed(field="key") @ImplicitActivation public final Collection<Element> elements = null;
		@Incrementing public final Map<String, Integer> elementsInc = null;

		public Container(String key) {
			this.key = key;
		}
	}
	
	public Container sut;
	
	@Before
	public void setupSut() throws DatabaseNotReachedException {
		this.resetQueryCount();
		sut = new Container("key");
		sut.activate("elements");
		this.assertHadAQuery();
		for(int i = 1 ; i <= 10; ++i) {
			sut.elements.add(new Element("E" + i));
			sut.elementsInc.put("E" + i, i);
		}
		sut.store();
		this.assertHadAQuery();
	}

	@After public void deleteSut() throws DatabaseNotReachedException {
		sut.delete();
	}
	
	@Test
	public void hasFamily() {
		assertEquals(CollectionColumnFamily.class, sut.elements.getClass());
		assertEquals(MapColumnFamily.class, sut.elementsInc.getClass());
	}
	
	@Persisting
	public static class ContainerSettingNonNull{
		@Key public final String key = "key";
		@Indexed(field="key") public final Collection<Element> elements = new ArrayList<CollectionStorageTest.Element>();
	}
	@Test(expected=IllegalArgumentException.class)
	public void notSettingNull() {
		new ContainerSettingNonNull();
	}
	
	@Persisting
	public static class ContainerNotIndexed{
		@Key public final String key = "key";
		public final Collection<Element> elements = null;
	}
	@Test(expected=IllegalArgumentException.class)
	public void notIndexed() {
		new ContainerNotIndexed();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void autoActivation() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		copy.activate();
		assertTrue(((ColumnFamily<Element>)copy.elements).isActivated());
	}
	@Test
	public void storeRetrieveElements() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		copy.activate("elements");
		this.assertHadAQuery();
		assertEquals(sut.elements.size(), copy.elements.size());
		this.assertHadNoQuery();
		for(int i = 1; i <= sut.elements.size(); ++i) {
			assertTrue(((CollectionColumnFamily<Element>)copy.elements).contains(new Element("E" + i)));
		}
		assertTrue(copy.elements.containsAll(sut.elements));
		this.assertHadNoQuery();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void storeRetrieveElementsFrom3To65() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		((ColumnFamily<Element>)copy.elements).activate("E3", "E65");
		this.assertHadAQuery();
		assertEquals(4, copy.elements.size());
		this.assertHadNoQuery();
		for(int i = 3; i <= 6; ++i) {
			assertTrue(((CollectionColumnFamily<Element>)copy.elements).contains(new Element("E" + i)));
		}
		this.assertHadNoQuery();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void storeRetrieveElementsFrom7ToEnd() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		((ColumnFamily<Element>)copy.elements).activate("E7", null);
		this.assertHadAQuery();
		assertEquals(3, copy.elements.size());
		this.assertHadNoQuery();
		for(int i = 7; i <= 9; ++i) {
			assertTrue(((CollectionColumnFamily<Element>)copy.elements).contains(new Element("E" + i)));
		}
		this.assertHadNoQuery();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void storeRetrieveElementsUpTo4() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		((ColumnFamily<Element>)copy.elements).activate(null, "E4");
		this.assertHadAQuery();
		assertEquals(5, copy.elements.size());
		this.assertHadNoQuery();
		for(int i = 1; i <= 4; ++i) {
			assertTrue(((CollectionColumnFamily<Element>)copy.elements).contains(new Element("E" + i)));
		}
		assertTrue(((CollectionColumnFamily<Element>)copy.elements).contains(new Element("E10")));
		this.assertHadNoQuery();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void change() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		copy.activate("elements");
		this.assertHadAQuery();
		copy.elements.remove(new Element("E4"));
		assertTrue(((ColumnFamily<Element>)copy.elements).wasDeleted("E4"));
		this.assertHadNoQuery();
	}

	
	@Persisting(table="TestContainer")
	public static class AddonlyContainer {
		@Key public final String key;
		@Indexed(field="key") @AddOnly public final Collection<Element> elements = null;

		public AddonlyContainer(String key) {
			this.key = key;
		}
	}
	@Test
	public void unauthorizedRemove() throws DatabaseNotReachedException {
		AddonlyContainer copy = new AddonlyContainer(sut.key);
		copy.activate("elements");
		this.assertHadAQuery();
		assertEquals(sut.elements.size(), copy.elements.size());
		assertTrue(!copy.elements.remove(new Element("E4")));
		assertTrue(copy.elements.contains(new Element("E4")));
		this.assertHadNoQuery();
	}
	
	@Test
	public void containsElementFromStore() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		this.assertHadNoQuery();
		assertEquals(0, copy.elements.size());
		this.assertHadNoQuery();
		assertTrue(!copy.elements.contains(new Element("GFYUBY")));
		this.assertHadNoQuery();
		assertFalse(((CollectionColumnFamily<Element>)copy.elements).containsInStore(new Element("GFYUBY")));
		assertTrue(copy.elements.isEmpty());
		this.assertHadAQuery();
		assertFalse(copy.elements.contains(new Element("E6")));
		this.assertHadNoQuery();
		assertTrue(((CollectionColumnFamily<Element>)copy.elements).containsInStore(new Element("E6")));
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
		assertFalse(((ColumnFamily<Element>)copy.elementsInc).wasDeleted("E4"));
		assertTrue(((ColumnFamily<Element>)copy.elementsInc).wasChanged("E4"));
		assertEquals(5, ((ColumnFamily<Element>)copy.elementsInc).getIncrement("E4"));
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
		((ColumnFamily<Element>)copy.elements).assertIsActivated(" testing legalActivityCheck");
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void illegalActivityCheck() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		copy.activate("elements");
		((ColumnFamily<Element>)copy.elements).assertIsActivated("testing illegalActivityCheck");
	}
}
