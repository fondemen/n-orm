package com.googlecode.n_orm;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.googlecode.n_orm.IncrementException;
import com.googlecode.n_orm.Incrementing;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.cf.MapColumnFamily;




public class IncrementsTest {

	@Persisting
	public static class Element {
		@Key public int key;
		public int notIncrmenting;
		@Incrementing(mode=Incrementing.Mode.Free) public int free;
		@Incrementing public int incrementing;
		@Incrementing(mode=Incrementing.Mode.DecrementOnly) public int decrementing;
		@Incrementing public Map<String, Short> incrementingFamily = new HashMap<String, Short>();
		@Incrementing(mode=Incrementing.Mode.Free) public Map<String, Short> freeFamily = new HashMap<String, Short>();
		@Incrementing(mode=Incrementing.Mode.DecrementOnly) public Map<String, Short> decrementingFamily = new HashMap<String, Short>();
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
	
	@Test(expected=IncrementException.class)
	public void decrementAnIncrementOnlyProp() {
		Element e = new Element(1);
		e.incrementing = -10;
	}
	
	@Test(expected=IncrementException.class)
	public void decrementAnIncrementOnlyPropByMin() {
		Element e = new Element(1);
		e.incrementing -= 10;
	}
	
	@Test
	public void decrementAnIncrementOnlyPropNotChecked() {
		IncrementManagement.setImmedatePropertyCheck(false);
		try {
			Element e = new Element(1);
			e.incrementing -= 10;
		} finally {
			IncrementManagement.setImmedatePropertyCheck(true);
		}
	}
	
	@Test(expected=IncrementException.class)
	public void incrementAnDecrementOnlyProp() {
		Element e = new Element(1);
		e.decrementing = 10;
	}
	
	@Test
	public void incrementDecrement() {
		Element e = new Element(1);
		assertFalse(e.getIncrements().containsKey("free"));
		e.free = 3;
		e.updateFromPOJO();
		assertTrue(e.getIncrements().containsKey("free"));
		assertEquals(3l, e.getIncrements().get("free").longValue());
		assertFalse(e.getPropertiesColumnFamily().changedKeySet().contains("free"));
		e.getIncrements().clear();
		assertFalse(e.getIncrements().containsKey("free"));
		e.free = -1;
		e.updateFromPOJO();
		assertEquals(-4l, (long)e.getIncrements().get("free").longValue());
	}
	
	@Test(expected=IncrementException.class)
	public void decrementCol() {
		Element e = new Element(1);
		e.incrementingFamily.put("val", (short)3);
		e.updateFromPOJO();
		e.incrementingFamily.put("val", (short)2);
		e.updateFromPOJO();
	}
	
	@Test(expected=IncrementException.class)
	public void negativeDecrementCol() {
		Element e = new Element(1);
		e.incrementingFamily.put("val", (short)-3);
		e.updateFromPOJO();
	}
	
	@Test(expected=IncrementException.class)
	public void positiceIncrementCol() {
		Element e = new Element(1);
		e.decrementingFamily.put("val", (short)2);
		e.updateFromPOJO();
	}
	
	@Test(expected=IncrementException.class)
	public void incrementCol() {
		Element e = new Element(1);
		e.decrementingFamily.put("val", (short)-3);
		e.updateFromPOJO();
		e.decrementingFamily.put("val", (short)-2);
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
