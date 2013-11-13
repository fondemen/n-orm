package com.googlecode.n_orm;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.operations.ImportExport;
import com.googlecode.n_orm.query.SearchableClassConstraintBuilder;

public class ImportExportTest {

	private static final String BOOKS_SER_FILE = "books.ser";
	
	public ImportExportTest() throws Exception {
		StoreTestLauncher.registerStorePropertiesForInnerClasses(getClass());
		Map<String, Object> props = StoreTestLauncher.INSTANCE.prepare(this.getClass());
		StoreSelector.getInstance().setPropertiesFor(BookStore.class, props);
		StoreSelector.getInstance().setPropertiesFor(Book.class, props);
		StoreSelector.getInstance().setPropertiesFor(Novel.class, props);
	}

	private BookStore bssut = new BookStore("testbookstore");

	@After
	public void deleteSerialization() {
		File ser = new File(BOOKS_SER_FILE);
		if (ser.exists())
			ser.delete();
	}

	@After
	@Before
	public void deleteElements() throws Exception {
		this.deleteAll(Element.class);
	}

	@After
	@Before
	public void deleteBooks() throws Exception {
		this.deleteAll(Book.class);
	}

	@After
	@Before
	public void deleteBookStore() throws Exception {
		this.deleteAll(BookStore.class);
	}
	
	public <T extends PersistingElement> void deleteAll(Class<T> clazz) throws Exception {
		while (StorageManagement.findElements().ofClass(clazz).any() != null) {
			StorageManagement.findElements().ofClass(clazz)
			.withAtMost(Integer.MAX_VALUE).elements().forEach(new Process<T>() {
				
				@Override
				public void process(T element) throws Throwable {
					element.delete();
				}
			});
		}
	}

	@Test
	public void importExport() throws IOException, ClassNotFoundException,
			DatabaseNotReachedException, InterruptedException {
		// Reusable query
		SearchableClassConstraintBuilder<Book> query = StorageManagement
				.findElements().ofClass(Book.class).andActivateAllFamilies()
				.withAtMost(1000).elements();

		Book b2 = new Book(bssut, new Date(12121212), new Date());
		b2.setNumber((short) 100);
		b2.store();
		Book b3 = new Book(new BookStore("rfgbuhfgj"), new Date(123456789),
				new Date());
		b3.store();

		// Original collection
		Set<Book> knownBooks = query.go();
		assertFalse(knownBooks.isEmpty());
		assertEquals(knownBooks.size(), query.count());

		// Exporting collection directly from store
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		ObjectOutputStream oout = new ObjectOutputStream(bout);
		query.exportTo(oout);
		oout.close();

		// Deleting collection from base
		for (Book book : knownBooks) {
			book.delete();
		}
		assertEquals(0, query.count());
		// Simulating new session by emptying the cache
		KeyManagement.getInstance().cleanupKnownPersistingElements();

		// Importing stored elements
		ByteArrayInputStream fis = new ByteArrayInputStream(bout.toByteArray());
		ImportExport.importPersistingElements(fis);
		fis.close();
		Thread.sleep(100);
		assertEquals(knownBooks.size(), query.count());

		// Checking database
		Set<Book> newKnownBooks = query.go(); // Caches found elements
		assertEquals(knownBooks, newKnownBooks);
		for (Book knownBook : knownBooks) {
			Book newKnownBook = StorageManagement
					.getElementUsingCache(knownBook); // The element activated
														// by the last
														// query.go()
			assertNotSame(knownBook, newKnownBook);
			assertEquals(knownBook, newKnownBook);
			assertEquals(knownBook.getBookStore(), newKnownBook.getBookStore());
			assertEquals(knownBook.getNumber(), newKnownBook.getNumber());
		}
	}

	@Test
	public void checkSerializeBook() throws DatabaseNotReachedException,
			IOException, ClassNotFoundException, InterruptedException {
		Book current;
		Iterator<Book> it;

		Book b2 = new Book(bssut, new Date(12121212), new Date());
		b2.store();
		Book b3 = new Book(new BookStore("rfgbuhfgj"), new Date(123456789),
				new Date());
		b3.store();

		SearchableClassConstraintBuilder<Book> searchQuery = StorageManagement
				.findElements().ofClass(Book.class).withAtMost(1000).elements();

		File f = new File(BOOKS_SER_FILE);
		f.delete();
		assertFalse(f.exists());

		// Serialize in a file
		FileOutputStream fos = new FileOutputStream(f);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		searchQuery.exportTo(oos);
		oos.close();

		// Test if the file has been created
		assertTrue(f.exists());

		NavigableSet<Book> originalBooks = searchQuery.go();
		it = originalBooks.iterator();
		while (it.hasNext()) {
			current = it.next();
			current.delete();
		}

		KeyManagement.getInstance().cleanupKnownPersistingElements();

		FileInputStream fis = new FileInputStream(f);
		ImportExport.importPersistingElements(fis);
		fis.close();
		Thread.sleep(100);
		NavigableSet<Book> unserializedBooks = searchQuery.go();

		assertEquals(originalBooks, unserializedBooks);
	}

	@Test
	public void checkUnserializeBook() throws DatabaseNotReachedException,
			IOException, ClassNotFoundException, InterruptedException {
		Book current;
		Iterator<Book> it;

		Book b2 = new Book(bssut, new Date(12121212), new Date());
		b2.store();
		Book b3 = new Book(new BookStore("rfgbuhfgj"), new Date(123456789),
				new Date());
		b3.store();

		SearchableClassConstraintBuilder<Book> searchQuery = StorageManagement
				.findElements().ofClass(Book.class).withAtMost(1000).elements();

		File f = new File(BOOKS_SER_FILE);

		// Serialize in a file
		FileOutputStream fos = new FileOutputStream(f);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		long exported = searchQuery.exportTo(oos);
		oos.close();
		assertEquals(searchQuery.count(), exported);

		// Test if the file has been created
		assertTrue(f.exists());
		NavigableSet<Book> originalBooks = searchQuery.go();
		long originalCount = searchQuery.count();

		it = originalBooks.iterator();
		while (it.hasNext()) {
			current = it.next();
			current.delete();
		}
		assertEquals(0, searchQuery.count());

		KeyManagement.getInstance().cleanupKnownPersistingElements();

		FileInputStream fis = new FileInputStream(f);
		long imported = ImportExport
				.importPersistingElements(fis);
		Thread.sleep(100);
		fis.close();

		assertEquals(exported, imported);
		assertEquals(originalCount, searchQuery.count());
	}

	@Test
	public void checkUnserializeBookAndBookStore()
			throws DatabaseNotReachedException, IOException,
			ClassNotFoundException, InterruptedException {
		PersistingElement current;
		Iterator<PersistingElement> it;

		Book b2 = new Book(bssut, new Date(12121212), new Date());
		b2.store();
		BookStore bs2 = new BookStore("rfgbuhfgj");
		bs2.store();
		Book b3 = new Book(bs2, new Date(123456789), new Date());
		b3.store();
		
		Thread.sleep(100);

		SearchableClassConstraintBuilder<BookStore> searchQuery1 = StorageManagement
				.findElements().ofClass(BookStore.class).withAtMost(1000)
				.elements();
		SearchableClassConstraintBuilder<Book> searchQuery2 = StorageManagement
				.findElements().ofClass(Book.class).withAtMost(1000).elements();

		File f = new File(BOOKS_SER_FILE);

		// Serialize in a file
		ObjectOutputStream fos = new ObjectOutputStream(new FileOutputStream(f));
		long exported = searchQuery1.exportTo(fos) + searchQuery2.exportTo(fos);
		fos.close();
		assertEquals(searchQuery1.count() + searchQuery2.count(), exported);

		// Test if the file has been created
		assertTrue(f.exists());

		TreeSet<PersistingElement> original = new TreeSet<PersistingElement>();
		original.addAll(searchQuery1.go());
		original.addAll(searchQuery2.go());
		long originalCount = searchQuery1.count() + searchQuery2.count();

		it = original.iterator();
		while (it.hasNext()) {
			current = it.next();
			current.delete();
		}
		assertEquals(0, searchQuery1.count() + searchQuery2.count());

		KeyManagement.getInstance().cleanupKnownPersistingElements();

		FileInputStream fis = new FileInputStream(f);
		long imported = ImportExport
				.importPersistingElements(fis);
		Thread.sleep(100);
		fis.close();

		assertEquals(exported, imported);
		assertEquals(originalCount, searchQuery1.count() + searchQuery2.count());
	}
	
	@Persisting
	public static class Element {
		private static final long serialVersionUID = -179946847012789575L;
		@Key public String key;
		public String name;
		public Set<String> set = null;
		public Map<String,String> map = null;
		public Element(String key) {
			this.key = key;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
	}

	@Test
	public void checkUnserializeExistingBookAndBookStore()
			throws DatabaseNotReachedException, IOException,
			ClassNotFoundException, InterruptedException {
		Element bs1 = new Element("bs1");
		bs1.setName("bs1");
		bs1.set.add("k11"); bs1.set.add("k12");
		bs1.map.put("km11", "valkm11"); bs1.map.put("km12", "valkm12");
		bs1.store();
		Element bs2 = new Element("bs2"); bs2.setName("bs2"); bs2.set.add("k21"); bs2.set.add("k22");
		bs2.store();
		Element bs3 = new Element("bs3"); bs3.setName(null);
		bs3.store();
		
		SearchableClassConstraintBuilder<Element> searchQuery1 = StorageManagement
				.findElements().ofClass(Element.class).withAtMost(1000)
				.elements();

		File f = new File(BOOKS_SER_FILE);

		// Serialize in a file
		ObjectOutputStream fos = new ObjectOutputStream(new FileOutputStream(f));
		long exported = searchQuery1.exportTo(fos);
		fos.close();
		assertEquals(searchQuery1.count(), exported);

		// Test if the file has been created
		assertTrue(f.exists());

		TreeSet<PersistingElement> original = new TreeSet<PersistingElement>();
		original.addAll(searchQuery1.go());
		long originalCount = searchQuery1.count();
		
		// Changing some data a little bit
		bs1.setName(null);
		bs1.set.remove("k11"); bs1.set.add("k13");
		bs1.map.put("km11", "valkm11bis"); bs1.map.remove("km12"); bs1.map.put("km13", "km13val");
		bs1.store();
		bs2.setName("2sb");bs2.store();
		bs3.setName("3sb");bs3.store();

		KeyManagement.getInstance().cleanupKnownPersistingElements();

		FileInputStream fis = new FileInputStream(f);
		long imported = ImportExport
				.importPersistingElements(fis);
		Thread.sleep(100);
		fis.close();

		assertEquals(exported, imported);
		assertEquals(originalCount, searchQuery1.count());

		// Data in store should be that data restored from serialization
		Map<String, String> treeMap = new TreeMap<String, String>();
		
		assertNull(bs1.getName());
		assertEquals(new TreeSet<String>(Arrays.asList("k12","k13")), bs1.set);
		treeMap.clear();
		treeMap.put("km11", "valkm11bis"); treeMap.put("km13", "km13val");
		assertEquals(treeMap, bs1.map);
		bs1.activate("set", "map");
		assertEquals("bs1", bs1.getName());
		assertEquals(new TreeSet<String>(Arrays.asList("k11","k12")), bs1.set);
		treeMap.clear();
		treeMap.put("km11", "valkm11"); treeMap.put("km12", "valkm12");
		assertEquals(treeMap, bs1.map);
		
		assertEquals("2sb", bs2.getName());
		assertEquals(new TreeSet<String>(Arrays.asList("k21","k22")), bs2.set);
		treeMap.clear();
		assertEquals(treeMap, bs2.map);
		bs2.activate("set","map");
		assertEquals("bs2", bs2.getName());
		assertEquals(new TreeSet<String>(Arrays.asList("k21","k22")), bs2.set);
		assertEquals(treeMap, bs2.map);
		
		assertEquals("3sb", bs3.getName());
		assertEquals(new TreeSet<String>(), bs3.set);
		bs3.activate("set");
		assertNull(bs3.getName());
		assertEquals(new TreeSet<String>(), bs3.set);
	}
	
	@Test(timeout=300000)
	public void longList() throws Exception {
		final int iterations = 5000;

		SearchableClassConstraintBuilder<Element> searchQuery = StorageManagement
				.findElements().ofClass(Element.class).andActivate()
				.withAtMost(iterations+1).elements();
		
		Process<Element> deleteAll = new Process<Element>() {

			@Override
			public void process(Element element) throws Throwable {
				element.delete();
			}
		};
		searchQuery.forEach(deleteAll);
		assertEquals(0, searchQuery.count());
		
		for(int i = 0; i < iterations; ++i) {
			Element e = new Element("e" + i);
			e.setName("e" + i + "name");
			e.store();
		}
		assertEquals(iterations, searchQuery.count());
		
		for(int i = 0; i < iterations; ++i) {
			Element e = new Element("e" + i);
			assertTrue(e.exists());
		}

		File f = new File(BOOKS_SER_FILE);

		// Serialize in a file
		ObjectOutputStream fos = new ObjectOutputStream(new FileOutputStream(f));
		long exported = searchQuery.exportTo(fos);
		fos.close();
		assertEquals(iterations, exported);

		// Test if the file has been created
		assertTrue(f.exists());
		
		searchQuery.forEach(deleteAll);
		assertEquals(0, searchQuery.count());

		KeyManagement.getInstance().cleanupKnownPersistingElements();

		FileInputStream fis = new FileInputStream(f);
		long imported = ImportExport.importPersistingElements(fis);
		fis.close();

		assertEquals(iterations, imported);
		assertEquals(iterations, searchQuery.count());
		
		for(int i = 0; i < iterations; ++i) {
			Element e = new Element("e" + i);
			e.activate();
			assertTrue(e.exists());
			assertEquals("e" + i + "name", e.getName());
				
		}
	}

}
