package com.mt.storage;

import static org.junit.Assert.assertEquals;
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
		StoreSelector.aspectOf().setPropertiesFor(BookStore.class, props);
		StoreSelector.aspectOf().setPropertiesFor(Book.class, props);
	}
	
	private BookStore psut = null;
	private Book vsut = null;
	
	@Before
	public void storeSUTs() throws DatabaseNotReachedException {
		boolean changed = false;
		
		if (psut == null) {
			 psut = new BookStore("testbookstore");
			 psut.setName("book name");
			 changed = true;
		}
		
		if (changed || vsut == null) {
			vsut = new Book(psut, new Date(1234567890), new Date(1234567890));
			vsut.setNumber((short)12);
			vsut.store();
		}
	}
	
	public void deleteProject() throws DatabaseNotReachedException {
		if (psut != null) {
			psut.delete();
			psut = null;
		}
	}
	 
	 @Test public void hbaseProjectRetrieve() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("testbookstore");
		 p.activateSimpleProperties();
		 assertEquals("book name", p.getName());
	 }
	 
	 @Test public void hbaseProjectSetNull() throws DatabaseNotReachedException {
		 psut.setName(null);
		 psut.store();
		 BookStore p = new BookStore("testbookstore");
		 p.activateSimpleProperties();
		 assertNull(p.getName());
		 deleteProject();
	 }
	
	 @Test public void hbaseProjectDeletion() throws DatabaseNotReachedException {
		 deleteProject();
		 BookStore p = new BookStore("testbookstore");
		 p.activateSimpleProperties();
		 assertNull(p.getName());
	 }
	
	 @Test public void hbaseProjectAccessFromVisit() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("testbookstore");
		 Book v = new Book(p, new Date(1234567890), new Date(1234567890));
		 v.activateSimpleProperties();
		 assertSame(p, v.getBookStore());
		 assertEquals("book name", v.getBookStore().getName());
		 assertEquals("book name", p.getName());
	 }
	 
	 @Test(expected=IllegalStateException.class) public void hbaseVisitWithNoProject() {
		 new Book(null, new Date(1234567890), new Date(1234567890));
	 }
	
	 @Test public void hbaseUnactivatedProjectAccessFromVisit() throws DatabaseNotReachedException {
		 BookStore p = new BookStore("testbookstore");
		 Book v = new Book(p, new Date(1234567890), new Date(1234567890));
		 assertSame(p, v.getBookStore());
		 assertNull(v.getBookStore().getName());
		 assertNull(p.getName());
	 }
	
	 @Test public void hbaseProjectDeletionAndthenAccess() throws DatabaseNotReachedException {
		 deleteProject();
		 this.storeSUTs();
		 BookStore p = new BookStore("testbookstore");
		 Book v = new Book(p, new Date(1234567890), new Date(1234567890));
		 v.activateSimpleProperties();
		 assertSame(p, v.getBookStore());
		 assertEquals("book name", v.getBookStore().getName());
		 assertEquals("book name", p.getName());
	 }

}
