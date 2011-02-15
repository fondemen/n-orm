package com.mt.storage;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(Parameterized.class)
public class CollectionStorageTest extends StoreTestLauncher {
	@Parameters
	public static Collection<Object[]> testedStores() throws Exception {
		return StoreTestLauncher.getTestedStores();
	}
	
	public CollectionStorageTest(Properties props) throws Exception {
		super(props);
		
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
		@Indexed(field="key") @Incrementing public final Collection<Element> elementsInc = null;

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
			sut.elementsInc.add(new Element(i, "E" + i));
		}
		sut.store();
		this.assertHadAQuery();
	}
	
	@After public void deleteSut() throws DatabaseNotReachedException {
		sut.delete();
	}
	
	@Test
	public void hasFamily() {
		assertEquals(ColumnFamily.class, sut.elements.getClass());
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
			assertTrue(((ColumnFamily<Element>)copy.elements).contains(new Element("E" + i)));
		}
		assertTrue(copy.elements.containsAll(sut.elements));
		this.assertHadNoQuery();
	}
	
	@Test
	public void storeRetrieveElementsFrom3To65() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		((ColumnFamily<Element>)copy.elements).activate("E3", "E65");
		this.assertHadAQuery();
		assertEquals(4, copy.elements.size());
		this.assertHadNoQuery();
		for(int i = 3; i <= 6; ++i) {
			assertTrue(((ColumnFamily<Element>)copy.elements).contains(new Element("E" + i)));
		}
		this.assertHadNoQuery();
	}
	
	@Test
	public void storeRetrieveElementsFrom7ToEnd() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		((ColumnFamily<Element>)copy.elements).activate("E7", null);
		this.assertHadAQuery();
		assertEquals(3, copy.elements.size());
		this.assertHadNoQuery();
		for(int i = 7; i <= 9; ++i) {
			assertTrue(((ColumnFamily<Element>)copy.elements).contains(new Element("E" + i)));
		}
		this.assertHadNoQuery();
	}
	
	@Test
	public void storeRetrieveElementsUpTo4() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		((ColumnFamily<Element>)copy.elements).activate(null, "E4");
		this.assertHadAQuery();
		assertEquals(5, copy.elements.size());
		this.assertHadNoQuery();
		for(int i = 1; i <= 4; ++i) {
			assertTrue(((ColumnFamily<Element>)copy.elements).contains(new Element("E" + i)));
		}
		assertTrue(((ColumnFamily<Element>)copy.elements).contains(new Element("E10")));
		this.assertHadNoQuery();
	}
	
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
		assertTrue(!((ColumnFamily<Element>)copy.elements).containsInStore(new Element("GFYUBY")));
		assertTrue(copy.elements.isEmpty());
		this.assertHadAQuery();
		assertTrue(!copy.elements.contains(new Element("E6")));
		this.assertHadNoQuery();
		assertTrue(((ColumnFamily<Element>)copy.elements).containsInStore(new Element("E6")));
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
	
	@Test
	public void increment() throws DatabaseNotReachedException {
		Container copy = new Container(sut.key);
		copy.activate("elementsInc");
		this.assertHadAQuery();
		assertTrue(copy.elementsInc.add(new Element(9,"E4")));
		assertFalse(((ColumnFamily<Element>)copy.elementsInc).wasDeleted("E4"));
		assertTrue(((ColumnFamily<Element>)copy.elementsInc).wasChanged("E4"));
		assertEquals(5, ((ColumnFamily<Element>)copy.elementsInc).getIncrement("E4"));
		this.assertHadNoQuery();
		copy.store();
		copy = new Container(sut.key);
		copy.activate("elementsInc");
		assertEquals(9, ((ColumnFamily<Element>)copy.elementsInc).get("E4").anint);
	}
}
