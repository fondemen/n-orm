package com.googlecode.n_orm.sample.businessmodel;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.cf.SetColumnFamily;

/**
 * Sample tests to learn how n-orm works.
 * In case those tests do not pass, try cleaning the project:<ul>
 * <li>with maven: mvn clean
 * <li>with eclipse: menu Project/Clean
 * </ul>
 */
public class BasicTest extends HBaseTestLauncher {
	
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
			bsut.store();
		}
	}
	
	@AfterClass
	public static void vacuumStore() {
		for (BookStore bs : StorageManagement.findElements().ofClass(BookStore.class).withAtMost(10000).elements().go()) {
			bs.delete();
		}
		for (Book b : StorageManagement.findElements().ofClass(Book.class).withAtMost(10000).elements().go()) {
			b.delete();
		}
		//Novel is subclass of Book:
		// emptying the Book table should also make the Novel table empty 
		assertNull(StorageManagement.findElements().ofClass(Novel.class).any());
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
		 assertEquals("turing str. 41", p.getAddress());
	 }
	 
	 @Test public void unknownBookStoreRetrieve() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("gdcfknueghficlnehfuci");
		 p.activate();
		 assertNull(p.getAddress());
		 assertFalse( p.existsInStore());
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
	 }
	 
	 @Test(expected=IllegalStateException.class) public void bookWithNoBookStoreSet() {
		 Book b = new Book(bssut, "testtitle", new Date(1234567890));
		 b.setReceptionDate(null);
		 b.activate();
	 }
	 
	 @Test(expected=IllegalStateException.class) public void bookWithNoBookStoreStored() {
		 new Book(null, "testtitle", new Date(1234567890)).store();
	 }
	 
	 @Test(expected=IllegalStateException.class) public void bookWithNoBookStoreActivated() {
		 new Book(null, "testtitle", new Date(1234567890)).activate();
	 }
	
	 @Test public void unactivatedBookStoreAccessFromBook() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("testbookstore");
		 Book v = new Book(p, "testtitle", new Date(1234567890));
		 assertSame(p, v.getBookStore());
		 assertNull(v.getBookStore().getAddress());
		 assertNull(p.getAddress());
	 }
	
	 @Test public void bookStoreDeletionAndthenAccess() throws DatabaseNotReachedException {
		 deleteBookstore();
		 this.storeSUTs();
		 BookStore p = new BookStore("testbookstore");
		 Book v = new Book(p, "testtitle", new Date(1234567890));
		 v.activate();
		 assertSame(p, v.getBookStore());
		 assertEquals("turing str. 41", v.getBookStore().getAddress());
		 assertEquals("turing str. 41", p.getAddress());
	 }
	 
	 @Test public void searchAllBooks() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, "testtitle2", new Date());
		 b2.store();
		 Book b3 = new Book(new BookStore("rfgbuhfgj"), "testtitle3", new Date());
		 b3.store();
		 
		 Set<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withAtMost(1000).elements().go();		 
		 b2.delete();
		 b3.delete();
		 
		 assertEquals(3, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(b2));
		 assertTrue(storeBooks.contains(b3));
		 
		 checkOrder(storeBooks);
	 }

	public void checkOrder(Set<? extends PersistingElement> elements) {
		PersistingElement last = null;
		 for (PersistingElement elt : elements) {
			if (last != null) assertTrue(last.compareTo(elt) <= 0);
			last = elt;
		}
	}
	 
	 @Test public void searchBook() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, "testtitle2", new Date());
		 b2.store();
		 Book b3 = new Book(new BookStore("rfgbuhfgj"), "testtitle3", new Date());
		 b3.store();
		 
		 Set<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withKey("bookStore").setTo(bssut).withAtMost(1000).elements().go();		 
		 b2.delete();
		 b3.delete();
		 
		 assertEquals(2, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(b2));
		 assertFalse(storeBooks.contains(b3));
		 
		 checkOrder(storeBooks);
	 }
	 
	 @Test public void searchBookWithMinTitle() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, "testtitle2", new Date());
		 b2.store();
		 
		 Set<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withKey("bookStore").setTo(bssut).withKey("title").greaterOrEqualsThan("testtitle1").withAtMost(1000).elements().go();		 
		 b2.delete();
		 
		 assertEquals(1, storeBooks.size());
		 assertFalse(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(b2));
		 
		 Iterator<Book> ib = storeBooks.iterator();
		 Book fb = ib.next();
		 assertEquals(b2, fb);
		 
		 //Unfortunately
		 assertNotSame(b2, fb);
	 }
	 
	 @Test public void searchBookWithMaxTitle() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, "testtitle2", new Date());
		 b2.store();
		 
		 Set<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withKey("bookStore").setTo(bssut).withKey("title").lessOrEqualsThan("testtitle1").withAtMost(1000).elements().go();		 
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
	 
	 @Test public void getSubClass() throws DatabaseNotReachedException {
		 Novel n = new Novel(bssut, "noveltitle", new Date());
		 n.store();

		 Set<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withAtMost(1000).elements().go();		 
		 
		 assertEquals(2, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(n));
	 }
	 
	 @Test public void serialize() throws DatabaseNotReachedException, IOException, ClassNotFoundException {
		assertEquals(SetColumnFamily.class, bsut.getBookStore().getBooks().getClass());
		//As such, it is necessarily the case that:
		assertFalse(ColumnFamily.class.isAssignableFrom(bsut.getBookStore().getBooks().getClass()));
		
		bsut.setPOJO(true); //WARNING: column family changes won't be detected anymore !
		assertFalse(ColumnFamily.class.isAssignableFrom(bsut.getBookStore().getBooks().getClass()));
		
		try {
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
		} finally {
			bsut.setPOJO(false);
			assertFalse(ColumnFamily.class.isAssignableFrom(bsut.getBookStore().getBooks().getClass()));
		}
	 }

}
