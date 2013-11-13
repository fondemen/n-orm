package com.googlecode.n_orm.performance;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.Book;
import com.googlecode.n_orm.BookStore;
import com.googlecode.n_orm.CloseableIterator;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.Novel;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.Process;
import com.googlecode.n_orm.RealBook;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.StoreTestLauncher;

public class BasicPerformanceTest {

	public BasicPerformanceTest() {
		Map<String, Object> props = StoreTestLauncher.INSTANCE.prepare(this.getClass());
		StoreSelector.getInstance().setPropertiesFor(BookStore.class, props);
		StoreSelector.getInstance().setPropertiesFor(Book.class, props);
		StoreSelector.getInstance().setPropertiesFor(Novel.class, props);
	}
	
	private BookStore bssut = null;
	private Book bsut = null;
	
	public ExecutorService exec = null;
	
	@BeforeClass
	@AfterClass
	public static void vacuumDB() throws DatabaseNotReachedException {
		ExecutorService exec = Executors.newFixedThreadPool(100);
		for (Class<?> clazz : new Class<?> [] {BookStore.class, Book.class, Novel.class}) {
			@SuppressWarnings("unchecked")
			CloseableIterator<? extends PersistingElement> found = StorageManagement.findElements().ofClass((Class<? extends PersistingElement>)clazz).withAtMost(10000).elements().iterate();
			try {
				while (found.hasNext()) {
					final PersistingElement elt = found.next();
					exec.submit(new Runnable () {

						@Override
						public void run() {
							elt.delete();
						}
						
					});
				}
			} finally {
				found.close();
			}
		}
		exec.shutdown();
		try {
			exec.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			fail(e.getMessage());
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
	
	@Before
	public void createExecutor() {
		this.exec = Executors.newFixedThreadPool(100);
	}
	
	@After
	public void stopExecutor() throws InterruptedException {
		this.exec.shutdown();
		this.exec.awaitTermination(5, TimeUnit.SECONDS);
	}
	
	public void deleteBookstore() throws DatabaseNotReachedException {
		if (bssut != null) {
			bssut.delete();
			bssut = null;
		}
	}
	 
	 @Test public void insert1kEmptyBS() throws DatabaseNotReachedException {
		 
		 for(int i = 0; i < 1000; i++) {
			 exec.submit(new Runnable() {

				@Override
				public void run() {
					BookStore p = new BookStore("testbookstore");
					p.activate();
				}
				 
			 });
		 }

	 }
	 
	 @Test public void insert1kNonEmptyBS() throws DatabaseNotReachedException {
		 for(int i = 0; i < 1000; i++) {
			 final int index = i;
			 exec.submit(new Runnable() {

				@Override
				public void run() {
					BookStore p = new BookStore("testbookstore");
					p.setName("Hello, I'm BS #"+index);
					p.activate();
				}
				 
			 });
		 }
	 }
	 
	 @Test public void insertAndStore1kNonEmptyBS() throws DatabaseNotReachedException {
		 for(int i = 0; i < 1000; i++) {
			 final int index = i;
			 exec.submit(new Runnable() {

				@Override
				public void run() {
					BookStore p = new BookStore("testbookstore");
					p.setName("Hello, I'm BS #"+index);
					p.store();
				}
				 
			 });
		 }
	 }
	
	 @Test public void insertAnddelete1kBS() throws DatabaseNotReachedException {
		 for(int i = 0; i < 1000; i++) {
			 final int index = i;
			 exec.submit(new Runnable() {

				@Override
				public void run() {
					BookStore p = new BookStore("testbookstore");
					p.setName("Hello, I'm BS #"+index);
					p.store();
					 p.delete();
				}
				 
			 });
		 }
	 }
	
	 @Test public void insertAnddelete1kBWithBS() throws DatabaseNotReachedException {
		 for(int i = 0; i < 10000; i++) {
			 final int index = i;
			 exec.submit(new Runnable() {

				@SuppressWarnings("deprecation")
				@Override
				public void run() {
					RealBook b = new RealBook(bssut, new Date(2012, 1, index % 30), new Date(2042, index % 12, 12), "Hello"+index, index);
					b.store();
					b.activate();
					bssut.store();
				}
				 
			 });
			 
		 }
	 }
	 
	 @SuppressWarnings("deprecation")
	@Test public void searchAmong1kBooks() throws DatabaseNotReachedException {
		 this.insertAnddelete1kBWithBS();
		 CloseableIterator<RealBook> storeBooks = StorageManagement.findElements().ofClass(RealBook.class)
					.withKey("bookStore").setTo(bssut)
					.withKey("sellerDate").lessOrEqualsThan(new Date(2014, 1, 1))
					.withAtMost(1000).elements().iterate();

		 int count = 0;
		 try {
			 while(storeBooks.next() != null) {
				 count++;
			 }
		 } catch (NoSuchElementException x) {
		 } finally {
			 storeBooks.close();
		 }
		 
		 assertEquals(1000, count);
		 
	 }
	 
	 @Test public void simultaneousActivateAndInsert() throws Throwable {
		int turns = 1000;
		ExecutorService exec = Executors.newFixedThreadPool(100);
		Collection<Future<?>> futures = new ArrayList<Future<?>>(turns*3);
		for(int i = 0; i < turns; i++) {
			 final int index = i;
			 futures.add(exec.submit(new Runnable() {

				@SuppressWarnings("deprecation")
				@Override
				public void run() {
					RealBook b = new RealBook(bssut, new Date(2012, 1, index % 30), new Date(2042, index % 12, 12), "Hello"+index, index);
					b.store();
					bssut.store();
				}
				 
			 }));
			 futures.add(exec.submit(new Runnable() {

				@SuppressWarnings("deprecation")
				@Override
				public void run() {
					RealBook b = new RealBook(bssut, new Date(2012, 1, index % 30), new Date(2042, index % 12, 12), "Hello"+index, index);
					b.activate();
				}
				 
			 }));
			 if (i%2 == 0)
				 futures.add(exec.submit(new Runnable() {
	
					@SuppressWarnings("deprecation")
					@Override
					public void run() {
						 final int [] count = {0};
						 int treated;
						try {
							treated = StorageManagement.findElements().ofClass(RealBook.class)
										.withKey("bookStore").setTo(bssut)
										.withKey("sellerDate").lessOrEqualsThan(new Date(2014, 1, 1))
										.withAtMost(1000).elements().forEach(new Process<RealBook>() {
										private static final long serialVersionUID = 8646643213678l;

											@Override
											public void process(RealBook element) throws Throwable {
												count[0]++;
											}
										}).getElementsTreated();
							assertTrue(treated == count[0]);
						} catch (Exception e) {
							e.printStackTrace();
							throw new RuntimeException(e);
						}
					}
					 
				 }));
		 }
		
		exec.shutdown();
		boolean running = exec.awaitTermination(5, TimeUnit.MINUTES);
		
		for (Future<?> future : futures) {
			if (future.isDone()) {
				try {
					future.get();
				} catch (ExecutionException x) {
					throw x.getCause();
				}
			}
		}
		
		assertTrue(running);
	 }
	 

}
