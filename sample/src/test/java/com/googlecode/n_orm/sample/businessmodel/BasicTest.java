package com.googlecode.n_orm.sample.businessmodel;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PersistingMixin;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.cf.SetColumnFamily;
import com.googlecode.n_orm.hbase.Store;
import com.googlecode.n_orm.query.SearchableClassConstraintBuilder;

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
		 
		 SearchableClassConstraintBuilder<Book> query = StorageManagement.findElements().ofClass(Book.class).withAtMost(1000).elements();
		 Set<Book> storeBooks = query.go();
		 long count = query.count();
		 b2.delete();
		 b3.delete();

		 assertEquals(3, count);
		 assertEquals(count, storeBooks.size());
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
		 b2.setNumber((short) 18);
		 b2.store();
		 Book b3 = new Book(new BookStore("rfgbuhfgj"), "testtitle3", new Date());
		 b3.store();
		 
		 //Simulates a new session by emptying elements cache
		 KeyManagement.getInstance().cleanupKnownPersistingElements();
		 
		 NavigableSet<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withKey("bookStore").setTo(bssut).withAtMost(1000).elements().go();		 
		 b2.delete();
		 b3.delete();
		 
		 assertEquals(2, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(b2));
		 assertFalse(storeBooks.contains(b3));

		 assertEquals(bsut, storeBooks.first());//bsut has a lower title
		 assertEquals(b2, storeBooks.last());
		 
		 assertEquals(0, storeBooks.last().getNumber());
		 assertFalse((short)0 == b2.getNumber()); //Just to test the test is well written
		 assertNull(storeBooks.last().getBookStore().getAddress()); //Activation is (automatically) propagated to simple persisting elements ; here, no activation required
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
		 									.andActivate().go();		 
		 b2.delete();
		 b3.delete();
		 
		 assertEquals(2, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(b2));
		 assertFalse(storeBooks.contains(b3));

		 assertEquals(bsut, storeBooks.first());//bsut has a lower title
		 assertEquals(b2, storeBooks.last());
		 
		 assertEquals(b2.getNumber(), storeBooks.last().getNumber());
		 assertFalse((short)0 == b2.getNumber()); //Just to test the test is well written
		 assertEquals(bssut.getAddress(), storeBooks.last().getBookStore().getAddress()); //Activation is (automatically) propagated to simple persisting elements
		 assertNotNull(bssut.getAddress()); //Just to test the test is well written
		 
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
	 }
	 
	 @Test public void searchBookWithBookstoreKey() throws DatabaseNotReachedException {
		 Book b2 = new Book(bssut, "testtitle2", new Date());
		 b2.store();
		 
		 Set<Book> storeBooks = StorageManagement.findElements().ofClass(Book.class).withKey("bookStore").isAnElement().withKey("name").greaterOrEqualsThan("test").and().withAtMost(1000).elements().go();		 
		 b2.delete();
		 
		 assertEquals(2, storeBooks.size());
		 assertTrue(storeBooks.contains(bsut));
		 assertTrue(storeBooks.contains(b2));
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
	 
	 @Test
	 public void compression() throws IOException {
		//src/test/resources/com/googlecode/n_orm/sample/businessmodel/store.properties defines the HBase store
		com.googlecode.n_orm.storeapi.Store ast = StoreSelector.getInstance().getStoreFor(Book.class);
		if (! (ast instanceof Store)) //In case you've changed the default store
			return;
		Store st = (Store) ast;
		//src/test/resources/com/googlecode/n_orm/sample/businessmodel/store.properties defines the compression property
		assertEquals("gz", st.getCompression());
		//Checking with HBase that the property column family is actually stored in GZ
		HTableDescriptor td = st.getAdmin().getTableDescriptor(Bytes.toBytes(PersistingMixin.getInstance().getTable(Book.class) /*could be bsut.getTable()*/));
		//Getting the property CF descriptor
		HColumnDescriptor pd = td.getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.GZ, pd.getCompression());
	 }

}
