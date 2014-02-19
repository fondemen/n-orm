package com.googlecode.n_orm;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.ProcessTest.InrementNovel;
import com.googlecode.n_orm.cf.ColumnFamily;

public class BasicTest {

	public BasicTest() {
		Properties props = StoreTestLauncher.INSTANCE.prepare(this.getClass());
		StoreSelector.getInstance().setPropertiesFor(BookStore.class, props);
		StoreSelector.getInstance().setPropertiesFor(Book.class, props);
		StoreSelector.getInstance().setPropertiesFor(Novel.class, props);
	}
	
	private BookStore bssut = null;
	private Book bsut = null;
	
	@BeforeClass
	@AfterClass
	public static void vacuumDB() throws DatabaseNotReachedException {
		for (Class<?> clazz : new Class<?> [] {BookStore.class, Book.class, Novel.class}) {
			@SuppressWarnings("unchecked")
			CloseableIterator<? extends PersistingElement> found = StorageManagement.findElements().ofClass((Class<? extends PersistingElement>)clazz).withAtMost(10000).elements().iterate();
			try {
				while (found.hasNext()) {
					found.next().delete();
				}
			} finally {
				found.close();
			}
		}
		KeyManagement.getInstance().cleanupKnownPersistingElements();
	}
	
	@Before
	public void storeSUTs() throws DatabaseNotReachedException {
		vacuumDB();
		//if (bssut == null) {
			 bssut = new BookStore("testbookstore");
			 bssut.setName("bookstore name");
			 bssut.store();
		//}
		
		//if (bsut == null) {
			bsut = new Book(bssut, new Date(1234567890), new Date(1234567890));
			bsut.setNumber((short)12);
			bsut.store();
			assertTrue(bsut.getPropertiesColumnFamily().isActivated());
		//}
	}
	
	public void deleteBookstore() throws DatabaseNotReachedException {
		if (bssut != null) {
			bssut.delete();
			bssut = null;
		}
	}
	 
	 @Test public void bookStoreRetrieve() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("testbookstore");
		 p.activate();
		 assertEquals("bookstore name", p.getName());
		 assertTrue( p.exists());
		 assertTrue( p.existsInStore());
	 }
	 
	 @Test public void unknownBookStoreRetrieve() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("gdcfknueghficlnehfuci");
		 p.activate();
		 assertNull(p.getName());
		 assertFalse( p.exists());
		 assertFalse( p.existsInStore());
	 }
	 
	 @Test public void bookStoreSetNull() throws DatabaseNotReachedException {
		 bssut.setName(null);
		 bssut.store();
		 BookStore p = new BookStore("testbookstore");
		 p.activate();
		 assertNull(p.getName());
	 }
	
	 @Test public void bookStoreDeletion() throws DatabaseNotReachedException {
		 deleteBookstore();
		 BookStore p = new BookStore("testbookstore");
		 p.activate();
		 assertNull(p.getName());
	 }
	
	 @Test public void bookStoreAccessFromBook() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("testbookstore");
		 Book v = new Book(p, new Date(1234567890), new Date(1234567890));
		 v.activate();
		 assertSame(p, v.getBookStore());
		 v.getBookStore().activate();
		 assertEquals("bookstore name", v.getBookStore().getName());
		 assertEquals("bookstore name", p.getName());
	 }
	 
	 @Test(expected=Test.None.class) public void bookWithNoBookStore() {
		 Book b = new Book(null, new Date(1234567890), new Date(1234567890));
		 b.setReceptionDate(null);
	 }
	 
	 @Test(expected=IllegalStateException.class) public void bookWithNoBookStoreSet() {
		 Book b = new Book(bssut, new Date(1234567890), new Date(1234567890));
		 b.setReceptionDate(null);
		 b.activate();
	 }
	 
	 @Test(expected=IllegalStateException.class) public void bookWithNoBookStoreStored() {
		 new Book(null, new Date(1234567890), new Date(1234567890)).store();
	 }
	 
	 @Test(expected=IllegalStateException.class) public void bookWithNoBookStoreActivated() {
		 new Book(null, new Date(1234567890), new Date(1234567890)).activate();
	 }
	
	 @Test public void unactivatedBookStoreAccessFromBook() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("testbookstore");
		 Book v = new Book(p, new Date(1234567890), new Date(1234567890));
		 assertSame(p, v.getBookStore());
		 assertNull(v.getBookStore().getName());
		 assertNull(p.getName());
	 }
	
	 @Test public void bookStoreDeletionAndthenAccess() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("testbookstore");
		 Book v = new Book(p, new Date(1234567890), new Date(1234567890));
		 v.activate();
		 assertSame(p, v.getBookStore());
		 p.activate();
		// assertEquals("bookstore name", v.getBookStore().getName());
		 assertEquals("bookstore name", p.getName());
	 }
	 
	 @Test public void searchAllBooks() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, new Date(123456789), new Date());
		 b2.store();
		 Book b3 = new Book(new BookStore("rfgbuhfgj"), new Date(123456789), new Date());
		 b3.store();
		 
		 Set<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withAtMost(1000).elements().go();	
		 long count = StorageManagement.findElements().ofClass(Book.class).withAtMost(1000).elements().count();	 
		 b2.delete();
		 b3.delete();

		 assertEquals(3, count);
		 assertEquals(3, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(b2));
		 assertTrue(storeBooks.contains(b3));
		 
		 checkOrder(storeBooks);
	 }
	 
	 @Test public void checkNpeIncrementsBook() throws IOException, ClassNotFoundException  {
		 Book b2 = new Book(bssut, new Date(12121212), new Date());
		 b2.store();
		 
		 ByteArrayOutputStream baos = new ByteArrayOutputStream();
		 ObjectOutputStream oos = new ObjectOutputStream(baos);
		 
		 oos.writeObject(b2);
		 
		 ObjectInputStream bais = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
		 Book usBook = (Book) bais.readObject();
		 usBook.store();
	 }

	public void checkOrder(Set<? extends PersistingElement> elements) {
		PersistingElement last = null;
		 for (PersistingElement elt : elements) {
			if (last != null) assertTrue(last.compareTo(elt) <= 0);
			last = elt;
		}
	}
	 
	 @Test public void searchBook() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, new Date(123456789), new Date());
		 b2.setNumber((short) 12);
		 b2.store();
		 Book b3 = new Book(new BookStore("rfgbuhfgj"), new Date(123456789), new Date());
		 b3.store();
		 
		 //Simulates a new session by emptying elements cache
		 KeyManagement.getInstance().cleanupKnownPersistingElements();
		 
		 NavigableSet<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withKey("bookStore").setTo(bssut).withAtMost(1000).elements().go();		 
		 long count = StorageManagement.findElements().ofClass(Book.class).withKey("bookStore").setTo(bssut).withAtMost(1000).elements().count();
		 b2.delete();
		 b3.delete();

		 

		 assertEquals(2, count);
		 assertEquals(2, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(b2));
		 assertFalse(storeBooks.contains(b3));

		 assertEquals(b2, storeBooks.first());//b2 has a lower sellerDate
		 assertEquals(bsut, storeBooks.last());
		 
		 //Not activated
		 assertNotSame(b2, storeBooks.first());
		 assertEquals(0, storeBooks.first().getNumber());
		 assertFalse((short)0 == b2.getNumber()); //Just to test the test is well written
		 assertNull(storeBooks.first().getBookStore().getName());
		 
		 checkOrder(storeBooks);
	 }
	 
	 @Test public void searchBookAndActivateProperties() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, new Date(123456789), new Date());
		 b2.setNumber((short) 12);
		 b2.store();
		 Book b3 = new Book(new BookStore("rfgbuhfgj"), new Date(123456789), new Date());
		 b3.store();
		 
		 //Simulates a new session by emptying elements cache
		 KeyManagement.getInstance().cleanupKnownPersistingElements();
		 
		 NavigableSet<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withKey("bookStore").setTo(bssut).withAtMost(1000).elements().andActivate().go();		 
		 b2.delete();
		 b3.delete();

		 assertEquals(2, storeBooks.size());
		 assertEquals(b2, storeBooks.first());//b2 has a lower sellerDate
		 assertEquals(bsut, storeBooks.last());
		 assertFalse(storeBooks.contains(b3));
		 
		 assertNotSame(b2, storeBooks.first());
		 assertEquals(b2.getNumber(), storeBooks.first().getNumber());
		 assertFalse((short)0 == storeBooks.first().getNumber()); //Just to test the test is well written
		 assertNull(storeBooks.first().getBookStore().getName()); //Activation is not (automatically) propagated to simple persisting elements
		 assertNotNull(bssut.getName()); //Just to test the test is well written
		 
		 checkOrder(storeBooks);
	 }
	 
	 @Test public void searchBookWithMinSellerDate() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, new Date(123456789), new Date());
		 b2.store();
		 
		 //Simulates a new session by emptying elements cache
		 KeyManagement.getInstance().cleanupKnownPersistingElements();
		 
		 Set<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withKey("bookStore").setTo(bssut).withKey("sellerDate").greaterOrEqualsThan(new Date(1000000000)).withAtMost(1000).elements().go();		 
		 b2.delete();
		 
		 assertEquals(1, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertFalse(storeBooks.contains(b2));
		 
		 Iterator<Book> ib = storeBooks.iterator();
		 Book fb = ib.next();
		 assertEquals(bsut, fb);
		 
		 //Unfortunately
		 assertNotSame(bsut, fb);
	 }
	 
	 @Test public void searchBookWithMaxSellerDate() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, new Date(123456789), new Date());
		 b2.store();
		 
		 //Simulates a new session by emptying elements cache
		 KeyManagement.getInstance().cleanupKnownPersistingElements();
		 
		 Set<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withKey("bookStore").setTo(bssut).withKey("sellerDate").lessOrEqualsThan(new Date(1000000000)).withAtMost(1000).elements().go();		 
		 b2.delete();
		 
		 assertEquals(1, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(b2));
		 
		 Iterator<Book> ib = storeBooks.iterator();
		 Book fb = ib.next();
		 assertEquals(b2, fb);
		 
		 //Unfortunately
		 assertNotSame(b2, fb);
	 }
	 
	 @Test public void searchBookWithBookstoreKey() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, new Date(123456789), new Date());
		 b2.store();
		 
		 //Simulates a new session by emptying elements cache
		 KeyManagement.getInstance().cleanupKnownPersistingElements();
		 
		 Set<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withKey("bookStore").isAnElement().withKey("hashcode").setTo("testbookstore").and().withAtMost(1000).elements().go();		 
		 b2.delete();
		 
		 assertEquals(2, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(b2));
	 }
	 
	 @Test public void getSubClass() throws DatabaseNotReachedException {
		 Novel n = new Novel(bssut, new Date(123456799), new Date());
		 n.store();
		 
		 try {
			 Set<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withAtMost(1000).elements().go();		 
			 
			 assertEquals(2, storeBooks.size());
			 assertTrue(storeBooks.contains(bsut));
			 assertTrue(storeBooks.contains(n));
		 } finally {
			 n.delete();
		 }
	 }

	 @Test public void activationFailedListener() {
		 bsut.activate(); //So that the next activateIfNotAlready does not trigger a data store request
		 
		 PersistingElementListener listener = EasyMock.createStrictMock(PersistingElementListener.class);
		 listener.activateInvoked(bsut, new TreeSet<ColumnFamily<?>>());
		 EasyMock.replay(listener);
		 bsut.addPersistingElementListener(listener);
		 bsut.activateIfNotAlready();
		 EasyMock.verify(listener);
	 }

	 @Test public void activationOKListener() {
		 PersistingElementListener listener = EasyMock.createStrictMock(PersistingElementListener.class);
		 listener.activateInvoked(bsut, new TreeSet<ColumnFamily<?>>());
		 ColumnFamily<?>[] activated = new ColumnFamily[] {bsut.getPropertiesColumnFamily()};
		 Capture<Set<ColumnFamily<?>>> famCap = new Capture<Set<ColumnFamily<?>>>();
		 Capture<Book> bookCap = new Capture<Book>();
		 listener.activated(EasyMock.capture(bookCap), EasyMock.capture(famCap));
		 EasyMock.replay(listener);
		 bsut.addPersistingElementListener(listener);
		 bsut.activate();
		 EasyMock.verify(listener);
		 assertEquals(bsut, bookCap.getValue());
		 assertArrayEquals(activated, famCap.getValue().toArray());
	 }

	 @Test public void storeFailedListener() {
		 bsut.store(); //So that the next store does not trigger a data store request (nothing changed)
		 
		 PersistingElementListener listener = EasyMock.createStrictMock(PersistingElementListener.class);
		 listener.storeInvoked(bsut);
		 EasyMock.replay(listener);
		 bsut.addPersistingElementListener(listener);
		 bsut.store();
		 EasyMock.verify(listener);
	 }

	 @Test public void storeOKListener() {
		 bsut.setNumber((short) (bsut.getNumber()+1)); //Makes a change so that the next store triggers a data store request
		 
		 PersistingElementListener listener = EasyMock.createStrictMock(PersistingElementListener.class);
		 listener.storeInvoked(bsut);
		 listener.stored(bsut);
		 EasyMock.replay(listener);
		 bsut.addPersistingElementListener(listener);
		 bsut.store();
		 EasyMock.verify(listener);
	 }

	 @Test public void storeRemovedListener() {
		 bsut.setNumber((short) (bsut.getNumber()+1)); //Makes a change so that the next store triggers a data store request
		 
		 PersistingElementListener listener = EasyMock.createStrictMock(PersistingElementListener.class);
		 EasyMock.replay(listener);
		 bsut.addPersistingElementListener(listener);
		 bsut.removePersistingElementListener(listener);
		 bsut.store();
		 EasyMock.verify(listener);
	 }

	 @Test public void storeFailed2Listeners() {
		 bsut.store(); //So that the next store does not trigger a data store request (nothing changed)

		 PersistingElementListener listener = EasyMock.createStrictMock(PersistingElementListener.class);
		 PersistingElementListener listener2 = EasyMock.createStrictMock(PersistingElementListener.class);
		 listener.storeInvoked(bsut);
		 EasyMock.replay(listener, listener2);
		 bsut.addPersistingElementListener(listener);
		 bsut.store();
		 EasyMock.verify(listener, listener2);
	 }
	 
	 @Test(timeout=500)
	 public void concurrentTest() throws InterruptedException {
		 final BookStore bs = StorageManagement.findElements().ofClass(BookStore.class).withKey("hashcode").setTo("testbookstore").any();
		 final BookStore bs2 = StorageManagement.findElements().ofClass(BookStore.class).withKey("hashcode").setTo("testbookstore").any();
		 assertSame(bs, bs2);
		 final BookStore[] bsth = new BookStore[1];
		 
		 Thread t = new Thread(new Runnable() {
			
			@Override
			public void run() {
				 bsth[0] = StorageManagement.findElements().ofClass(BookStore.class).withKey("hashcode").setTo("testbookstore").any();
			}
		}, "inside");
		 t.start();
		 
		 do {
			 Thread.sleep(50);
		 } while(t.isAlive());

		 assertNotSame(bssut, bsth[0]);
		 assertNotSame(bs, bsth[0]);
		 assertEquals(bssut, bsth[0]);
	 }
		
	@Persisting
	public static class EmptyClass {
		private static final long serialVersionUID = 8947335685993348699L;
		@Key public String key;
	}
	
	@Test
	public void exists() {
		EmptyClass sut = new EmptyClass();
		sut.key = "dummy key";
		assertFalse(sut.exists());
		sut.store();
		assertTrue(sut.exists());

		EmptyClass sut2 = new EmptyClass();
		sut2.key = sut.key;
		sut2.activate();
		assertTrue(sut2.exists());
		
		sut.delete();
		assertFalse(sut.exists());

		//Sadly: assertTrue(sut2.exists());
		assertFalse(sut2.existsInStore()); //Triggers a database query
		assertFalse(sut2.exists());
	}
	 
	 @Test/*(timeout=20000)*/ public void processAsync() throws DatabaseNotReachedException, InterruptedException, InstantiationException, IllegalAccessException {
		 Novel n1 = new Novel(bssut, new Date(123456799), new Date(0));
		 n1.attribute = 1;
		 n1.store();
		 Novel n2 = new Novel(bssut, new Date(123456799), new Date(1));
		 n2.attribute = 2;
		 n2.store();
		 
		 try {
			WaitingCallBack cb = new WaitingCallBack();
			StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().andActivate().remoteForEach(new InrementNovel(2), cb, 2, 20000);
			synchronized(cb) {
				cb.waitProcessCompleted();
			}

			n1.activate();
			n2.activate();
			assertEquals(3, n1.attribute);
			assertEquals(4, n2.attribute);
		 } finally {
			 n1.delete();
			 n2.delete();
		 }
	 }
}
