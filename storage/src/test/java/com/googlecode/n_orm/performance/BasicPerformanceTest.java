package com.googlecode.n_orm.performance;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.googlecode.n_orm.Book;
import com.googlecode.n_orm.BookStore;
import com.googlecode.n_orm.CloseableIterator;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.Novel;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.RealBook;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.ProcessTest.InrementNovel;
import com.googlecode.n_orm.StoreTestLauncher;
import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.query.SearchableClassConstraintBuilder;

public class BasicPerformanceTest {

	public BasicPerformanceTest() {
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
	 
	@Ignore
	 @Test public void insert1kEmptyBS() throws DatabaseNotReachedException {
		 
		 BookStore p;
		 
		 for(int i = 0; i < 1000; i++) {
			 p = new BookStore("testbookstore");
			 p.activate();
		 }

	 }
	 
	@Ignore
	 @Test public void insert1kNonEmptyBS() throws DatabaseNotReachedException {
		 BookStore p;
		 
		 for(int i = 0; i < 1000; i++) {
			 p = new BookStore("testbookstore");
			 p.setName("Hello, I'm BS #"+i);
			 p.activate();
		 }
	 }
	 
	@Ignore
	 @Test public void insertAndStore1kNonEmptyBS() throws DatabaseNotReachedException {
		 BookStore p;
		 
		 for(int i = 0; i < 1000; i++) {
			 p = new BookStore("testbookstore");
			 p.setName("Hello, I'm BS #"+i);
			 p.store();
		 }
	 }
	
	@Ignore
	 @Test public void insertAnddelete1kBS() throws DatabaseNotReachedException {
		 BookStore p;
		 
		 for(int i = 0; i < 1000; i++) {
			 p = new BookStore("testbookstore");
			 p.setName("Hello, I'm BS #"+i);
			 p.store();
			 p.delete();
		 }
	 }
	
	 @Test public void insertAnddelete1kBWithBS() throws DatabaseNotReachedException {
		 RealBook b;
		 for(int i = 0; i < 10000; i++) {
			 b = new RealBook(bssut, new Date(2012, 1, i % 30), new Date(2042, i % 12, 12), "Hello"+i, i);
			 b.store();
			 b.activate();
			 bssut.store();
		 }
	 }
	 
	 @Test public void searchAmong1kBooks() throws DatabaseNotReachedException {
		 CloseableIterator<RealBook> storeBooks = StorageManagement.findElements().ofClass(RealBook.class)
					.withKey("bookStore").setTo(bssut)
					.withKey("sellerDate").lessOrEqualsThan(new Date(2014, 1, 1))
					.withAtMost(10).elements().iterate();

		 int count = 0;
		 while(storeBooks.next() != null) {
			 count++;
		 }
		 assertEquals(1000, count);
		 
	 }
	 

}
