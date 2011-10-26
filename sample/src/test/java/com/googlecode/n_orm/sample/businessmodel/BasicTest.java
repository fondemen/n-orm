package com.googlecode.n_orm.sample.businessmodel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.NavigableSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

import com.googlecode.n_orm.CloseableIterator;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.Process;
import com.googlecode.n_orm.ProcessException;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.WaitingCallBack;
import com.googlecode.n_orm.query.SearchableClassConstraintBuilder;

/**
 * Sample tests to learn how n-orm works.
 * In case those tests do not pass, try cleaning the project:<ul>
 * <li>with maven: mvn clean
 * <li>with eclipse: menu Project/Clean
 * </ul>
 */
public class BasicTest {

	static {
		try {
			ClassLoader.getSystemClassLoader().loadClass("com.googlecode.n_orm.sample.businessmodel.HBaseTestLauncher").newInstance();
		} catch (Exception e) {
			//We are not using HBase ; no need to prepare it
		}
	}
	
	private BookStore bssut = null;
	private Book bsut = null;
	
	@Before
	public void storeSUTs() throws DatabaseNotReachedException {
		boolean changed = false;
		
		if (bssut == null) {
			 bssut = new BookStore("testbookstore");
			 bssut.setAddress("turing str. 41");
			 changed = true;
		}
		
		if (changed || bsut == null) {
			bsut = new Book(bssut, "testtitle", new Date(1234567890));
			bsut.setNumber((short)12);
			bsut.store(); //There is no need to store bssut here thanks to the @ImplicitActivation annotation on the Book.bookStore field
		}
	}
	
	@BeforeClass
	@AfterClass
	public static void vacuumStore() throws DatabaseNotReachedException, InterruptedException, ProcessException {
		StorageManagement.findElements().ofClass(BookStore.class).withAtMost(10000).elements().forEach(new Process<BookStore>() {
			
			@Override
			public void process(BookStore element) {
				element.delete();
			}
		}, 10, 10000);
		StorageManagement.findElements().ofClass(Book.class).withAtMost(10000).elements().forEach(new Process<Book>() {
			
			@Override
			public void process(Book element) {
				element.delete();
			}
		}, 10, 1000);
		//Novel is subclass of Book:
		// emptying the Book table should also make the Novel table empty 
		assertNull(StorageManagement.findElements().ofClass(Novel.class).any());
		
		//Cleaning cache to simulate new session
		KeyManagement.getInstance().cleanupKnownPersistingElements();
	}
	
	public void deleteBookstore() throws DatabaseNotReachedException {
		if (bssut != null) {
			bssut.delete();
			assertFalse(bssut.exists());
			bssut = null;
		}
	}
	 
	 @Test public void bookStoreRetrieve() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("testbookstore");
		 p.activate();
		 assertEquals("turing str. 41", p.getAddress());
		 assertTrue(p.exists()); //No data store request should be issued as activate was successful
	 }
	 
	 @Test public void unknownBookStoreRetrieve() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("gdcfknueghficlnehfuci");
		 p.activate();
		 assertNull(p.getAddress());
		 assertFalse( p.exists()); //No data store request should be issued as activate was successful
	 }
	 
	 @Test public void bookStoreSetNull() throws DatabaseNotReachedException {
		 bssut.setAddress(null);
		 bssut.store();
		 BookStore p = new BookStore("testbookstore");
		 p.activate();
		 assertNull(p.getAddress());
		 deleteBookstore();
	 }
	
	 @Test public void bookStoreDeletion() throws DatabaseNotReachedException {
		 deleteBookstore();
		 BookStore p = new BookStore("testbookstore");
		 p.activate();
		 assertNull(p.getAddress());
		 assertFalse(p.exists()); //No data store request should be issued as activate was unsuccessful
	 }
	
	 @Test public void bookStoreAccessFromBook() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("testbookstore");
		 Book v = new Book(p, "testtitle", new Date(1234567890));
		 v.activate();
		 assertSame(p, v.getBookStore());
		 assertEquals("turing str. 41", v.getBookStore().getAddress());
		 assertEquals("turing str. 41", p.getAddress());
	 }
	 
	 @Test(expected=Test.None.class) public void bookWithNoBookStore() {
		 Book b = new Book(null, "testtitle", new Date(1234567890));
		 b.setReceptionDate(null);
		 //No exception even though b is not "complete" (i.e. has not all its keys attributed) as not activate/store/exists requests are issued
	 }
	 
	 @Test(expected=IllegalStateException.class) public void bookWithNoBookStoreSet() {
		 Book b = new Book(bssut, "testtitle", new Date(1234567890));
		 b.setReceptionDate(null);
		 b.activate();
		 //b is not "complete" (i.e. has not all its keys attributed)
	 }
	 
	 @Test(expected=IllegalStateException.class) public void bookWithNoBookStoreStored() {
		 new Book(null, "testtitle", new Date(1234567890)).store();
		 //b is not "complete" (i.e. has not all its keys attributed) since all keys have to be non null
	 }
	 
	 @Test(expected=IllegalStateException.class) public void bookWithNoBookStoreActivated() {
		 new Book(null, "testtitle", new Date(1234567890)).activate();
		 //b is not "complete" (i.e. has not all its keys attributed) since all keys have to be non null
	 }
	
	 @Test public void unactivatedBookStoreAccessFromBook() throws DatabaseNotReachedException {
		 BookStore bs = new BookStore("testbookstore");
		 Book b = new Book(bs, "testtitle", new Date(1234567890));
		 assertSame(bs, b.getBookStore());
		 assertNull(b.getBookStore().getAddress());
		 assertNull(bs.getAddress());
	 }
	
	 @Test public void bookStoreDeletionAndthenAccess() throws DatabaseNotReachedException {
		 deleteBookstore();
		 this.storeSUTs();
		 BookStore bs = new BookStore("testbookstore");
		 Book b = new Book(bs, "testtitle", new Date(1234567890));
		 b.activate();
		 assertSame(bs, b.getBookStore());
		 assertEquals("turing str. 41", b.getBookStore().getAddress());
		 assertEquals("turing str. 41", bs.getAddress());
	 }
	 
	 @Test public void searchAllBooks() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, "testtitle2", new Date());
		 b2.store();
		 Book b3 = new Book(new BookStore("rfgbuhfgj"), "testtitle3", new Date());
		 b3.store();
		 
		 SearchableClassConstraintBuilder<Book> query = StorageManagement.findElements().ofClass(Book.class).withAtMost(1000).elements();
		 Set<Book> storeBooks = query.go();
		 long count = query.count();
		 
		 //Postamble before assertion so that it is always ran
		 b2.delete();
		 b3.delete();

		 assertEquals(3, count);
		 assertEquals(count, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(b2));
		 assertTrue(storeBooks.contains(b3));
		 
		 checkOrder(storeBooks);
	 }

	 /**
	  * Checks that a list is actually ordered
	  */
	public void checkOrder(Set<? extends PersistingElement> elements) {
		PersistingElement last = null;
		 for (PersistingElement elt : elements) {
			if (last != null) assertTrue(last.compareTo(elt) <= 0);
			//Comparison actually compares keys ; not that compareTo, equals, hashKey and toString are redefined depending on the key
			last = elt;
		}
	}
	 
	 @Test public void searchBook() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, "testtitle2", new Date());
		 b2.setNumber((short) 18);
		 b2.store();
		 Book b3 = new Book(new BookStore("rfgbuhfgj"), "testtitle3", new Date());
		 b3.store();
		 
		 //Simulates a new session by emptying elements cache
		 KeyManagement.getInstance().cleanupKnownPersistingElements();
		 
		 NavigableSet<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class)
		 										.withKey("bookStore").setTo(bssut)
		 										.withAtMost(1000).elements()
		 										.go(); //go is easier to write this test, but you should prefer iterate instead as it requests the data store clever

		 //Postamble before assertion so that it is always ran
		 b2.delete();
		 b3.delete();
		 StorageManagement.findElements().ofClass(BookStore.class).withKey("name").setTo("rfgbuhfgj").any().delete();
		 
		 assertEquals(2, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(b2));
		 assertFalse(storeBooks.contains(b3));

		 assertEquals(bsut, storeBooks.first());//bsut has a lower title
		 assertEquals(b2, storeBooks.last());
		 
		 assertEquals(0, storeBooks.last().getNumber());
		 assertFalse((short)0 == b2.getNumber()); //Just to test the test is well written
		 assertNull(storeBooks.last().getBookStore().getAddress()); //Activation is (automatically) propagated to simple persisting elements thanks to the @ImplicitActivation annotation on Book.bookStore ; here, no explicit activation required
		 assertNotNull(bssut.getAddress()); //Just to test the test is well written
		 
		 checkOrder(storeBooks);
	 }
	 
	 @Test public void searchBookAndActivateProperties() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, "testtitle2", new Date());
		 b2.setNumber((short) 18);
		 b2.store();
		 Book b3 = new Book(new BookStore("rfgbuhfgj"), "testtitle3", new Date());
		 b3.store();
		 
		 NavigableSet<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class)
		 									.withKey("bookStore").setTo(bssut)
		 									.withAtMost(1000).elements()
		 									.andActivate().go(); //go is easier to write this test, but you should prefer iterate instead as it requests the data store clever

		 //Postamble before assertion so that it is always ran
		 b2.delete();
		 b3.delete();

		 //Reminder
		 assertEquals(2, storeBooks.size());
		 assertEquals(bsut, storeBooks.first());//bsut has a lower title
		 assertEquals(b2, storeBooks.last());
		 
		 assertEquals(b2.getNumber(), storeBooks.last().getNumber()); //The found elements are activated at same time the request is issued
		 assertFalse((short)0 == b2.getNumber()); //Just to test the test is well written
		 assertEquals(bssut.getAddress(), storeBooks.last().getBookStore().getAddress()); //Activation is (automatically) propagated to simple persisting elements thanks to the @ImplicitActivation annotation on Book.bookStore
		 assertNotNull(bssut.getAddress()); //Just to test the test is well written
		 
		 checkOrder(storeBooks);
	 }
	 
	 @Test public void searchBookWithMinTitle() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, "testtitle2", new Date());
		 b2.store();
		 
		 CloseableIterator<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class)
		 							.withKey("bookStore").setTo(bssut)
		 							.withKey("title").greaterOrEqualsThan("testtitle1")
		 							.withAtMost(1000).elements().iterate();
		 
		 //Postamble before assertion so that it is always ran
		 b2.delete();
		 
		 assertTrue(storeBooks.hasNext());
		 Book found = storeBooks.next();
		 assertFalse(storeBooks.hasNext());
		 
		 assertEquals(b2, found);
		 
		 storeBooks.close(); //Avoid forgetting closing iterators !
	 }
	 
	 @Test public void searchBookWithMaxTitle() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, "testtitle2", new Date());
		 b2.store();
		 
		 CloseableIterator<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class)
		 							.withKey("bookStore").setTo(bssut)
		 							.withKey("title").lessOrEqualsThan("testtitle1")
		 							.withAtMost(1000).elements().iterate(); //go is easier to write this test, but you should prefer iterate instead as it requests the data store clever
		 
		 //Postamble before assertion so that it is always ran
		 b2.delete();
		 
		 assertTrue(storeBooks.hasNext());
		 Book found = storeBooks.next();
		 assertFalse(storeBooks.hasNext());
		 
		 assertEquals(bsut, found);
		 
		 storeBooks.close(); //Avoid forgetting closing iterators !
	 }
	 
	 @Test public void searchBookWithBookstoreKey() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, "testtitle2", new Date());
		 b2.store();
		 
		 Set<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class)
		 							.withKey("bookStore").isAnElement().withKey("name").greaterOrEqualsThan("test")
		 							.and().withAtMost(1000).elements().go(); //go is easier to write this test, but you should prefer iterate instead as it requests the data store clever		 
		 
		 //Postamble before assertion so that it is always ran
		 b2.delete();
		 
		 assertEquals(2, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(b2));
	 }
	 
	 @Test public void getSubClass() throws DatabaseNotReachedException {
		 Novel n = new Novel(bssut, "noveltitle", new Date());
		 n.store();

		 Set<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withAtMost(1000).elements().go(); //go is easier to write this test, but you should prefer iterate instead as it requests the data store clever		 
		 
		 assertEquals(2, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(n));
	 }

	 //Must have a default constructor (or no constructor...)
		public static class BookSetter implements Process<Book> {
		private static final long serialVersionUID = -7258359759953024740L;
			private short value;
		
			public BookSetter(short val) {
				this.value = val;
			}

			@Override
			public void process(Book element) {
				element.setNumber(this.value);
				element.store();
			}
		}
	 @Test(timeout=20000) public void testSetBooksWithMapReduce() throws DatabaseNotReachedException, InstantiationException, IllegalAccessException, InterruptedException {
		WaitingCallBack wc = new WaitingCallBack();
		StorageManagement.findElements().ofClass(Book.class).withKey("bookStore").setTo(bssut).withAtMost(10000).elements().andActivate().remoteForEach(new BookSetter((short) 50), wc, 100, 1000);
		//withAtMost is not necessary using HBase as com.googlecode.n_orm.hbase.Store implements com.googlecode.n_orm.storeapi.ActionnableStore
		wc.waitProcessCompleted(); //Nothing happens up to the end of the Map/Reduce task ; in case you do not use an com.googlecode.n_orm.storeapi.ActionnableStore, this action is performed on this thread, i.e. all elements are downloaded and iterated here
		assertNull(wc.getError());
		 
		boolean hasABook = false;
		for (Book b : bssut.getBooks()) {
			hasABook = true;
			b.activate();
			assertEquals(50, b.getNumber());
		}
		assertTrue("No book in test !", hasABook);
	 }
	 
	 @Test public void serialize() throws DatabaseNotReachedException, IOException, ClassNotFoundException {
		//Java serialization ; also tested successfully with GSon
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream oo = new ObjectOutputStream(out);
		oo.writeObject(bsut);
		oo.close();
		
		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		ObjectInputStream oi = new ObjectInputStream(in);
		Book b2 = (Book) oi.readObject();
		
		assertNotNull(b2);
		assertNotSame(bsut, b2);
		//Keys for both persisting objects are the same 
		assertEquals(bsut.getIdentifier(), b2.getIdentifier());
		
		assertEquals(bsut.getNumber(), b2.getNumber());
		assertNotSame(bsut.getBookStore(), b2.getBookStore());
		assertEquals(bsut.getBookStore().getName(), b2.getBookStore().getName());
		assertEquals(bsut.getBookStore().getAddress(), b2.getBookStore().getAddress());
		//As such:
		assertEquals(bsut.getBookStore(), b2.getBookStore());
		//As such:
		assertEquals(bsut, b2);
	 }
	 
	 @Ignore @Test public void importExport() throws IOException, ClassNotFoundException, DatabaseNotReachedException {
		 //Reusable query
		SearchableClassConstraintBuilder<Book> query = StorageManagement.findElements().ofClass(Book.class).andActivateAllFamilies().withAtMost(1000).elements();
		
		//Original collection
		Set<Book> knownBooks = query.go();
		assertFalse(knownBooks.isEmpty());
		assertEquals(knownBooks.size(), query.count());

		//Exporting collection directly from store
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		query.exportTo(out);
		
		//Deleting collection from base
		for (Book book : knownBooks) {
			book.delete();
		}
		assertEquals(0, query.count());
		//Simulating new session by emptying the cache
		KeyManagement.getInstance().cleanupKnownPersistingElements();
		
		//Importing stored elements
		StorageManagement.importPersistingElements(new ByteArrayInputStream(out.toByteArray()));
		assertEquals(knownBooks.size(), query.count());
		
		//Checking database
		Set<Book> newKnownBooks = query.go(); //Caches found elements
		assertEquals(knownBooks, newKnownBooks);
		for (Book knownBook : knownBooks) {
			Book newKnownBook = StorageManagement.getElementUsingCache(knownBook); //The element activated by the last query.go()
			assertNotSame(knownBook, newKnownBook);
			assertEquals(knownBook, newKnownBook);
			assertEquals(knownBook.getBookStore(), newKnownBook.getBookStore());
			assertEquals(knownBook.getTitle(), newKnownBook.getTitle());
		}
	 }

}
