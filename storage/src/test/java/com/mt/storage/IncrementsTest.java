package com.mt.storage;

import static org.junit.Assert.*;

import org.junit.Test;


public class IncrementsTest {

	@Persisting
	public static class Element {
		@Key public final int key;
		public int notIncrmenting;
		@Incrementing public int incrementing;
		@Indexed(field="key") @Incrementing public final ColumnFamily<Value> incrementingFamily = null;
		public Element(int key) {
			this.key = key;
		}
	}
	
	public static class Value {
		@Key public final String key;
		@Key public final short value;
		public Value(String key, short value) {
			this.key = key;
			this.value = value;
		}
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void decrement() {
		Element e = new Element(1);
		e.incrementing--;
	}
	
	@Test
	public void increment() {
		Element e = new Element(1);
		assertTrue(!e.getIncrements().containsKey("incrementing"));
		e.incrementing = 3;
		assertTrue(e.getIncrements().containsKey("incrementing"));
		assertEquals(3l, e.getIncrements().get("incrementing").longValue());
		assertTrue(! e.getProperties().changedKeySet().contains("incrementing"));
		e.getIncrements().clear();
		assertTrue(!e.getIncrements().containsKey("incrementing"));
		e.incrementing = 8;
		assertEquals(5l, (long)e.getIncrements().get("incrementing").longValue());
	}
	
	@Test
	public void decrementCol() {
		Element e = new Element(1);
		e.incrementingFamily.add(new Value("val", (short)3));
		assertTrue(!e.incrementingFamily.add(new Value("val", (short)2)));
	}
	
	@Test
	public void unusefulUpdtaeCol() {
		Element e = new Element(1);
		assertEquals(null, e.incrementingFamily.getIncrement("val"));
		assertTrue(e.incrementingFamily.add(new Value("val", (short)3)));
		assertEquals((short)3, e.incrementingFamily.getIncrement("val"));
		assertTrue(e.incrementingFamily.add(new Value("val", (short)3)));
		assertEquals((short)3, e.incrementingFamily.getIncrement("val"));
	}
	
	@Test
	public void usefulUpdtaeCol() {
		Element e = new Element(1);
		assertTrue(e.incrementingFamily.add(new Value("val", (short)3)));
		e.incrementingFamily.clearChanges();
		assertEquals(null, e.incrementingFamily.getIncrement("val"));
		assertTrue(e.incrementingFamily.add(new Value("val", (short)7)));
		assertEquals((short)4, e.incrementingFamily.getIncrement("val"));
	}
}
