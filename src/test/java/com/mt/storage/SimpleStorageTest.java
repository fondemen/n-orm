package com.mt.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mt.storage.conversion.ConversionTools;
import com.mt.storage.memory.Memory;

public class SimpleStorageTest {

	@Persisting(table = "SimpleElement")
	public static class SimpleElement {
		@SuppressWarnings("unused")
		@Key(order = 1)
		private final String key1;
		@Key(order = 2)
		public final String[] key2;
		public String prop1;
		public boolean prop2;
		public String nullProp;
		private String privProp;
		public byte[] bytesProp; 
		public int[] intsProp; 

		public SimpleElement(String key1, String[] key2) {
			super();
			this.key1 = key1;
			this.key2 = key2;
			this.prop1 = "";
			// this.prop2 = false;
		}

//		public String getKey1() {
//			return key1;
//		}

		public String[] getKey2() {
			return key2;
		}

		public String getPrivProp() {
			return privProp;
		}

		public void setPrivProp(String privProp) {
			this.privProp = privProp;
		}
	}

	private SimpleElement sut1;

	@Before
	public void createElements() throws DatabaseNotReachedException {
		this.sut1 = new SimpleElement("KEY1", new String[]{"KE", "Y2"});
		this.sut1.prop1 = "pro1value";
		this.sut1.prop2 = true;
		this.sut1.store();
		Memory.INSTANCE.resetQueries();
	}

	@After
	public void vacuumDb() {
		Memory.INSTANCE.reset();
	}
	
	@Test
	public void inexisting() throws DatabaseNotReachedException {
		SimpleElement unknown = new SimpleElement("guhkguilnu", new String [] {"gbuyikgnui", "yuihju"});
		assertTrue(Memory.INSTANCE.hadNoQuery());
		assertFalse(unknown.existsInStore());
		assertTrue(Memory.INSTANCE.hadAQuery());
		unknown.activate();
		assertTrue(Memory.INSTANCE.hadAQuery());
		assertFalse(unknown.existsInStore());
		assertTrue(Memory.INSTANCE.hadAQuery());
	}

	@Test
	public void getSutTable() {
		assertEquals("SimpleElement", this.sut1.getTable());
		assertTrue(Memory.INSTANCE.hadNoQuery());
	}

	@Test
	public void storeProperty() throws DatabaseNotReachedException {
		assertEquals("pro1value", ConversionTools.convert(String.class, Memory.INSTANCE.get(
				this.sut1.getTable(), this.sut1.getIdentifier(),
				PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "prop1")));
		Memory.INSTANCE.resetQueries();
		this.sut1.prop1 = "another prop1 value";
		assertTrue(Memory.INSTANCE.hadNoQuery());
		assertEquals("pro1value", ConversionTools.convert(String.class, Memory.INSTANCE.get(
				this.sut1.getTable(), this.sut1.getIdentifier(),
				PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "prop1")));
		Memory.INSTANCE.resetQueries();
		this.sut1.store();
		assertTrue(Memory.INSTANCE.hadAQuery());
		assertEquals("another prop1 value", ConversionTools.convert(String.class, Memory.INSTANCE.get(
				this.sut1.getTable(), this.sut1.getIdentifier(),
				PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "prop1")));
	}

	@Test
	public void soreKeyInProperties() {
		assertEquals("KEY1", ConversionTools.convert(String.class, Memory.INSTANCE.get(
				this.sut1.getTable(), this.sut1.getIdentifier(),
				PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "key1")));
		assertArrayEquals(new String [] {"KE", "Y2"}, ConversionTools.convert(String[].class, Memory.INSTANCE.get(
				this.sut1.getTable(), this.sut1.getIdentifier(),
				PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "key2")));
	}

	@Test
	public void retreive() throws DatabaseNotReachedException {
		SimpleElement sut2 = new SimpleElement("KEY1", new String[]{"KE", "Y2"});
		assertTrue(Memory.INSTANCE.hadNoQuery());
		sut2.activate();
		assertTrue(Memory.INSTANCE.hadAQuery());
		assertFalse(sut2.hasChanged());
		assertEquals("pro1value", sut2.prop1);
		assertTrue(Memory.INSTANCE.hadNoQuery());
	}

	@Test
	public void getNullProp() {
		assertNull(this.sut1.nullProp);
		assertTrue(Memory.INSTANCE.hadNoQuery());
	}

	@Test
	public void retreiveNullProp() throws DatabaseNotReachedException {
		assertTrue(Memory.INSTANCE.get(this.sut1.getTable(),
				this.sut1.getIdentifier(),
				PropertyManagement.PROPERTY_COLUMNFAMILY_NAME).containsKey(
				"prop1"));
		Memory.INSTANCE.resetQueries();
		this.sut1.prop1 = null;
		this.sut1.store();
		assertTrue(Memory.INSTANCE.hadAQuery());
		assertFalse(Memory.INSTANCE.get(this.sut1.getTable(),
				this.sut1.getIdentifier(),
				PropertyManagement.PROPERTY_COLUMNFAMILY_NAME).containsKey(
				"prop1"));
		Memory.INSTANCE.resetQueries();
		SimpleElement sut2 = new SimpleElement("KEY1", new String[]{"KE", "Y2"});
		assertTrue(Memory.INSTANCE.hadNoQuery());
		sut2.activate();
		assertTrue(Memory.INSTANCE.hadAQuery());
		assertEquals(null, sut2.prop1);
		assertTrue(Memory.INSTANCE.hadNoQuery());
	}

	@Test
	public void changedObject() {
		assertFalse(this.sut1.hasChanged());
		this.sut1.prop2 = false;
		this.sut1.storeProperties();
		assertTrue(this.sut1.hasChanged());
		assertTrue(Memory.INSTANCE.hadNoQuery());
	}

	@Test
	public void retreiveArrayProperty() throws DatabaseNotReachedException {
		this.sut1.intsProp = new int [] {1,2,3};
		assertTrue(Memory.INSTANCE.hadNoQuery());
		this.sut1.store();
		assertTrue(Memory.INSTANCE.hadAQuery());
		SimpleElement sut2 = new SimpleElement("KEY1", new String[]{"KE", "Y2"});
		sut2.activate();
		assertTrue(Memory.INSTANCE.hadAQuery());
		assertArrayEquals(this.sut1.intsProp, sut2.intsProp);
		assertTrue(Memory.INSTANCE.hadNoQuery());
	}

	@Test
	public void retreiveByteArrayProperty() throws DatabaseNotReachedException {
		this.sut1.bytesProp = new byte [] {1,2,3};
		assertTrue(Memory.INSTANCE.hadNoQuery());
		this.sut1.store();
		assertTrue(Memory.INSTANCE.hadAQuery());
		SimpleElement sut2 = new SimpleElement("KEY1", new String[]{"KE", "Y2"});
		sut2.activate();
		assertTrue(Memory.INSTANCE.hadAQuery());
		assertArrayEquals(this.sut1.bytesProp, sut2.bytesProp);
		assertTrue(Memory.INSTANCE.hadNoQuery());
	}

	@Test
	public void retreivePrivateProperty() throws DatabaseNotReachedException {
		this.sut1.setPrivProp("privatevalue");
		assertTrue(Memory.INSTANCE.hadNoQuery());
		this.sut1.store();
		assertTrue(Memory.INSTANCE.hadAQuery());
		SimpleElement sut2 = new SimpleElement("KEY1", new String[]{"KE", "Y2"});
		sut2.activate();
		assertTrue(Memory.INSTANCE.hadAQuery());
		assertEquals("privatevalue", sut2.getPrivProp());
		assertTrue(Memory.INSTANCE.hadNoQuery());
	}

	@Test
	public void changePrivateProperty() throws DatabaseNotReachedException {
		this.sut1.setPrivProp("privatevalue");
		this.sut1.store();
		this.sut1.activate();
		assertEquals("privatevalue", ConversionTools.convert(String.class, Memory.INSTANCE.get(
				this.sut1.getTable(), this.sut1.getIdentifier(),
				PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "privProp")));
	}

	@Test
	public void noChange() {
		assertEquals(new HashSet<String>(Arrays.asList(new String[] {})),
				this.sut1.getProperties().changedKeySet());
	}

	@Test
	public void partialChange() {
		this.sut1.nullProp = "not null!";
		this.sut1.setPrivProp("null");
		this.sut1.storeProperties();
		assertEquals(
				new HashSet<String>(Arrays.asList(new String[] { "privProp",
						"nullProp" })), this.sut1.getProperties()
						.changedKeySet());
	}

	@Test
	public void partialChangeWithDummyUpdate() {
		this.sut1.prop2 = true; // was already true
		this.sut1.nullProp = "not null!";
		this.sut1.setPrivProp("null");
		this.sut1.storeProperties();
		assertEquals(
				new HashSet<String>(Arrays.asList(new String[] { "privProp",
						"nullProp" })), this.sut1.getProperties()
						.changedKeySet());
	}

	@Persisting
	public static class DummyPersister {
		@Key
		public final String key = "singleton";
		public Object property;
	}

	@Test(expected = IllegalStateException.class)
	public void invalidProperty() {
		DummyPersister dp = new DummyPersister();
		dp.property = new Object();
	}
	
	@Persisting
	public static class IncrementingElement {
		@Key public final String key;
		@Incrementing public long lval;
		@Incrementing public int ival;
		@Incrementing public short sval;
		@Incrementing public byte bval;
		public IncrementingElement(String key) {
			this.key = key;
		}
	}
	@Test
	public void incrementingLong() throws DatabaseNotReachedException {
		IncrementingElement elt = new IncrementingElement("elt");
		elt.lval = 123l;
		elt.store();
		assertTrue(Memory.INSTANCE.hadAQuery());
		elt = new IncrementingElement("elt");
		elt.lval = 127l;
		elt.store();
		assertTrue(Memory.INSTANCE.hadAQuery());
		elt = new IncrementingElement("elt");
		elt.activate();
		assertTrue(Memory.INSTANCE.hadAQuery());
		assertEquals(250, elt.lval);
		assertTrue(Memory.INSTANCE.hadNoQuery());
		
		elt.lval += 51;
		elt.store();
		assertTrue(Memory.INSTANCE.hadAQuery());
		elt = new IncrementingElement("elt");
		elt.activate();
		assertTrue(Memory.INSTANCE.hadAQuery());
		assertEquals(301, elt.lval);
		assertTrue(Memory.INSTANCE.hadNoQuery());
		
	}
	@Test
	public void incrementingInt() throws DatabaseNotReachedException {
		IncrementingElement elt = new IncrementingElement("elt");
		elt.ival = 123;
		elt.store();
		assertTrue(Memory.INSTANCE.hadAQuery());
		elt = new IncrementingElement("elt");
		elt.ival = 127;
		elt.store();
		assertTrue(Memory.INSTANCE.hadAQuery());
		elt = new IncrementingElement("elt");
		elt.activate();
		assertTrue(Memory.INSTANCE.hadAQuery());
		assertEquals(250, elt.ival);
		assertTrue(Memory.INSTANCE.hadNoQuery());
	}
	@Test
	public void incrementingShort() throws DatabaseNotReachedException {
		IncrementingElement elt = new IncrementingElement("elt");
		elt.sval = 123;
		elt.store();
		assertTrue(Memory.INSTANCE.hadAQuery());
		elt = new IncrementingElement("elt");
		elt.sval = 127;
		elt.store();
		assertTrue(Memory.INSTANCE.hadAQuery());
		elt = new IncrementingElement("elt");
		elt.activate();
		assertTrue(Memory.INSTANCE.hadAQuery());
		assertEquals(250, elt.sval);
		assertTrue(Memory.INSTANCE.hadNoQuery());
	}
	@Test
	public void incrementingByte() throws DatabaseNotReachedException {
		IncrementingElement elt = new IncrementingElement("elt");
		elt.bval = 19;
		elt.store();
		assertTrue(Memory.INSTANCE.hadAQuery());
		elt = new IncrementingElement("elt");
		elt.bval = 3;
		elt.store();
		assertTrue(Memory.INSTANCE.hadAQuery());
		elt = new IncrementingElement("elt");
		elt.activate();
		assertTrue(Memory.INSTANCE.hadAQuery());
		assertEquals(22, elt.bval);
		assertTrue(Memory.INSTANCE.hadNoQuery());
	}
}
