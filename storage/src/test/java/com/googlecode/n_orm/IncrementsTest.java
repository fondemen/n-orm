package com.googlecode.n_orm;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.googlecode.n_orm.DecrementException;
import com.googlecode.n_orm.Incrementing;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.cf.MapColumnFamily;




public class IncrementsTest {

	@Persisting
	public static class Element {
		@Key public int key;
		public int notIncrmenting;
		@Incrementing public int incrementing;
		@Incrementing public Map<String, Short> incrementingFamily = new HashMap<String, Short>();
		public Element(int key) {
			this.key = key;
		}
	}
	
	public static class Value {
		@Key public String key;
		@Key public short value;
		public Value(String key, short value) {
			this.key = key;
			this.value = value;
		}
	}
	
	@Test
	public void increment() {
		Element e = new Element(1);
		assertFalse(e.getIncrements().containsKey("incrementing"));
		e.incrementing = 3;
		e.updateFromPOJO();
		assertTrue(e.getIncrements().containsKey("incrementing"));
		assertEquals(3l, e.getIncrements().get("incrementing").longValue());
		assertFalse(e.getPropertiesColumnFamily().changedKeySet().contains("incrementing"));
		e.getIncrements().clear();
		assertFalse(e.getIncrements().containsKey("incrementing"));
		e.incrementing = 8;
		e.updateFromPOJO();
		assertEquals(5l, (long)e.getIncrements().get("incrementing").longValue());
	}
	
	@Test(expected=DecrementException.class)
	public void decrementCol() {
		Element e = new Element(1);
		e.incrementingFamily.put("val", (short)3);
		e.updateFromPOJO();
		e.incrementingFamily.put("val", (short)2);
		e.updateFromPOJO();
	}
	
	@Test
	public void unusefulUpdtaeCol() {
		Element e = new Element(1);
		e.updateFromPOJO();
		assertEquals(null, e.getColumnFamily("incrementingFamily").getIncrement("val"));
		e.incrementingFamily.put("val", (short)3);
		e.updateFromPOJO();
		assertEquals((short)3, e.getColumnFamily("incrementingFamily").getIncrement("val"));
		e.incrementingFamily.put("val", (short)3);
		e.updateFromPOJO();
		assertEquals((short)3, e.getColumnFamily("incrementingFamily").getIncrement("val"));
	}
	
	@Test
	public void usefulUpdtaeCol() {
		Element e = new Element(1);
		e.incrementingFamily.put("val", (short)3);
		e.updateFromPOJO();
		e.getColumnFamily("incrementingFamily").clearChanges();
		assertEquals(null, e.getColumnFamily("incrementingFamily").getIncrement("val"));
		e.incrementingFamily.put("val", (short)7);
		e.updateFromPOJO();
		assertEquals((short)4, e.getColumnFamily("incrementingFamily").getIncrement("val"));
	}
}
