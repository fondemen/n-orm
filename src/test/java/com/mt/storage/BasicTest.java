package com.mt.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.Collection;
import java.util.Date;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(Parameterized.class)
public class BasicTest extends StoreTestLauncher {
	@Parameters
	public static Collection<Object[]> testedStores() throws Exception {
		return StoreTestLauncher.getTestedStores();
	}

	public BasicTest(Properties props) {
		super(props);
		StoreSelector.getInstance().setPropertiesFor(BookStore.class, props);
		StoreSelector.getInstance().setPropertiesFor(Book.class, props);
	}
	
	private BookStore bssut = null;
	private Book bsut = null;
	
	@Before
	public void storeSUTs() throws DatabaseNotReachedException {
		boolean changed = false;
		
		if (bssut == null) {
			 bssut = new BookStore("testbookstore");
			 bssut.setName("book name");
			 changed = true;
		}
		
		if (changed || bsut == null) {
			bsut = new Book(bssut, new Date(1234567890), new Date(1234567890));
			bsut.setNumber((short)12);
			bsut.store();
		}
	}
	
	public void deleteBookstore() throws DatabaseNotReachedException {
		if (bssut != null) {
			bssut.delete();
			bssut = null;
		}
	}
	 
	 @Test public void hbaseBookStoreRetrieve() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("testbookstore");
		 p.activateSimpleProperties();
		 assertEquals("book name", p.getName());
	 }
	 
	 @Test public void hbaseUnknownBookStoreRetrieve() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("gdcfknueghficlnehfuci");
		 p.activateSimpleProperties();
		 assertNull(p.getName());
		 assertFalse( p.existsInStore());
	 }
	 
	 @Test public void hbaseBookStoreSetNull() throws DatabaseNotReachedException {
		 bssut.setName(null);
		 bssut.store();
		 BookStore p = new BookStore("testbookstore");
		 p.activateSimpleProperties();
		 assertNull(p.getName());
		 deleteBookstore();
	 }
	
	 @Test public void hbaseBookStoreDeletion() throws DatabaseNotReachedException {
		 deleteBookstore();
		 BookStore p = new BookStore("testbookstore");
		 p.activateSimpleProperties();
		 assertNull(p.getName());
	 }
	
	 @Test public void hbaseBookStoreAccessFromBook() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("testbookstore");
		 Book v = new Book(p, new Date(1234567890), new Date(1234567890));
		 v.activateSimpleProperties();
		 assertSame(p, v.getBookStore());
		 assertEquals("book name", v.getBookStore().getName());
		 assertEquals("book name", p.getName());
	 }
	 
	 @Test(expected=IllegalStateException.class) public void hbaseBookWithNoBookStore() {
		 new Book(null, new Date(1234567890), new Date(1234567890));
	 }
	
	 @Test public void hbaseUnactivatedBookStoreAccessFromBook() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("testbookstore");
		 Book v = new Book(p, new Date(1234567890), new Date(1234567890));
		 assertSame(p, v.getBookStore());
		 assertNull(v.getBookStore().getName());
		 assertNull(p.getName());
	 }
	
	 @Test public void hbaseBookStoreDeletionAndthenAccess() throws DatabaseNotReachedException {
		 deleteBookstore();
		 this.storeSUTs();
		 BookStore p = new BookStore("testbookstore");
		 Book v = new Book(p, new Date(1234567890), new Date(1234567890));
		 v.activateSimpleProperties();
		 assertSame(p, v.getBookStore());
		 assertEquals("book name", v.getBookStore().getName());
		 assertEquals("book name", p.getName());
	 }

}
