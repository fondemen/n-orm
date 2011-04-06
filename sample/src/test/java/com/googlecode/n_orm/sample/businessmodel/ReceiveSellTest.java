package com.googlecode.n_orm.sample.businessmodel;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class ReceiveSellTest extends HBaseTestLauncher {

	private BookStore sut;
	
	@Before
	public void createBs() {
		sut = new BookStore("ACME books");
		sut.store();
	}
	
	@After
	public void cleanupDataStore() {
		for (Book b : sut.getBooks()) {
			b.delete();
		}
		sut.delete();
	}
	
	@Test
	public void gettingAndRetreivingABook() {
		Book b = sut.newBooks("n-orm for dummies", (short)3);
		assertNotNull(b);
		assertEquals(3, b.getNumber());
		assertTrue(sut.getBooks().contains(b));
		
		//Some more books received
		Book b2 = sut.newBooks("n-orm for dummies", (short)5);
		assertNotNull(b2);
		assertEquals(b, b2);
		assertEquals(8, b2.getNumber());
		assertTrue(sut.getBooks().contains(b2));
		
		b.activate();
		assertEquals(8, b2.getNumber());
		
		
		for (int i = 0; i < 5; ++i) {
			sut.sold(b);
			assertTrue(sut.getBooksToBeOrdered().isEmpty());
		}
		
		sut.sold(b);
		assertEquals(1, sut.getBooksToBeOrdered().size());
		assertTrue(sut.getBooksToBeOrdered().contains(b));
		assertTrue(sut.getBooksToBeOrdered().contains(b2));
	}
}
