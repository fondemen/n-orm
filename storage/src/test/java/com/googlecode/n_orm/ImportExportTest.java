package com.googlecode.n_orm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Test;

import com.googlecode.n_orm.operations.ImportExport;
import com.googlecode.n_orm.query.SearchableClassConstraintBuilder;

public class ImportExportTest {

	private static final String BOOKS_SER_FILE = "books.ser";


	private BookStore bssut = new BookStore("testbookstore");
	
	@After
	public void deleteSerialization() {
		File ser = new File(BOOKS_SER_FILE);
		if (ser.exists())
			ser.delete();
	}

	 
	 @Test public void importExport() throws IOException, ClassNotFoundException, DatabaseNotReachedException {
		 //Reusable query
		SearchableClassConstraintBuilder<Book> query = StorageManagement.findElements().ofClass(Book.class).andActivateAllFamilies().withAtMost(1000).elements();


		 Book b2 = new Book(bssut, new Date(12121212), new Date());
		b2.setNumber((short) 100);
		 b2.store();
		 Book b3 = new Book(new BookStore("rfgbuhfgj"), new Date(123456789), new Date());
		 b3.store();
		 
		//Original collection
		Set<Book> knownBooks = query.go();
		assertFalse(knownBooks.isEmpty());
		assertEquals(knownBooks.size(), query.count());

		//Exporting collection directly from store
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		query.exportTo(new ObjectOutputStream(out));
		
		//Deleting collection from base
		for (Book book : knownBooks) {
			book.delete();
		}
		assertEquals(0, query.count());
		//Simulating new session by emptying the cache
		KeyManagement.getInstance().cleanupKnownPersistingElements();
		
		//Importing stored elements
		ImportExport.importPersistingElements(new ByteArrayInputStream(out.toByteArray()));
		assertEquals(knownBooks.size(), query.count());
		
		//Checking database
		Set<Book> newKnownBooks = query.go(); //Caches found elements
		assertEquals(knownBooks, newKnownBooks);
		for (Book knownBook : knownBooks) {
			Book newKnownBook = StorageManagement.getElementUsingCache(knownBook); //The element activated by the last query.go()
			assertNotSame(knownBook, newKnownBook);
			assertEquals(knownBook, newKnownBook);
			assertEquals(knownBook.getBookStore(), newKnownBook.getBookStore());
			assertEquals(knownBook.getNumber(), newKnownBook.getNumber());
		}
	 }
	
	 @Test public void checkSerializeBook() throws DatabaseNotReachedException, IOException, ClassNotFoundException {
		 Book current;
		 Iterator<Book> it;

		 Book b2 = new Book(bssut, new Date(12121212), new Date());
		 b2.store();
		 Book b3 = new Book(new BookStore("rfgbuhfgj"), new Date(123456789), new Date());
		 b3.store();
		 
		 SearchableClassConstraintBuilder<Book> searchQuery = StorageManagement.findElements().ofClass(Book.class).withAtMost(1000).elements();
		 
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
		 while(it.hasNext())
		 {
			 current = it.next();
			 current.delete();
		 }
		 
		 KeyManagement.getInstance().cleanupKnownPersistingElements();

		 ImportExport.importPersistingElements(new FileInputStream(f));
		 NavigableSet<Book> unserializedBooks = searchQuery.go();
		 
		 assertEquals(originalBooks, unserializedBooks);
	 }
	 
	 @Test public void checkUnserializeBook() throws DatabaseNotReachedException, IOException, ClassNotFoundException {
		 Book current;
		 Iterator<Book> it;

		 Book b2 = new Book(bssut, new Date(12121212), new Date());
		 b2.store();
		 Book b3 = new Book(new BookStore("rfgbuhfgj"), new Date(123456789), new Date());
		 b3.store();
		 
		 SearchableClassConstraintBuilder<Book> searchQuery = StorageManagement.findElements().ofClass(Book.class).withAtMost(1000).elements();
		 
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
		 while(it.hasNext())
		 {
			 current = it.next();
			 current.delete();
		 }
		 assertEquals(0, searchQuery.count());
		 
		 KeyManagement.getInstance().cleanupKnownPersistingElements();

		 long imported = ImportExport.importPersistingElements(new FileInputStream(f));
		 
		 assertEquals(exported, imported);
		 assertEquals(originalCount, searchQuery.count());
	 }
	 
	 @Test public void checkUnserializeBookAndBookStore() throws DatabaseNotReachedException, IOException, ClassNotFoundException {
		 PersistingElement current;
		 Iterator<PersistingElement> it;

		 Book b2 = new Book(bssut, new Date(12121212), new Date());
		 b2.store();
		 BookStore bs2 = new BookStore("rfgbuhfgj");
		 bs2.store();
		Book b3 = new Book(bs2, new Date(123456789), new Date());
		 b3.store();

		 SearchableClassConstraintBuilder<BookStore> searchQuery1 = StorageManagement.findElements().ofClass(BookStore.class).withAtMost(1000).elements();
		 SearchableClassConstraintBuilder<Book> searchQuery2 = StorageManagement.findElements().ofClass(Book.class).withAtMost(1000).elements();
		 
		 File f = new File(BOOKS_SER_FILE);
		 
		 // Serialize in a file
		 ObjectOutputStream fos = new ObjectOutputStream( new FileOutputStream(f) );
		 long exported = searchQuery1.exportTo(fos) + searchQuery2.exportTo(fos);
		 fos.close();
		 assertEquals(searchQuery1.count()+searchQuery2.count(), exported);
		 
		 // Test if the file has been created
		 assertTrue(f.exists());
		 
		 TreeSet<PersistingElement> original = new TreeSet<PersistingElement>();
		 original.addAll(searchQuery1.go());
		 original.addAll(searchQuery2.go());
		 long originalCount = searchQuery1.count()+searchQuery2.count();

		 it = original.iterator();
		 while(it.hasNext())
		 {
			 current = it.next();
			 current.delete();
		 }
		 assertEquals(0, searchQuery1.count()+searchQuery2.count());
		 
		 KeyManagement.getInstance().cleanupKnownPersistingElements();

		 long imported = ImportExport.importPersistingElements(new FileInputStream(f));
		 
		 assertEquals(exported, imported);
		 assertEquals(originalCount, searchQuery1.count()+searchQuery2.count());
	 }

}
