package com.googlecode.n_orm;

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

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.Incrementing;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.memory.Memory;


public class SimpleStorageTest {

	@Persisting(table = "SimpleElement")
	public static class SimpleElement {
		private static final long serialVersionUID = 583478722942646042L;
		@SuppressWarnings("unused")
		@Key(order = 1)
		protected  String key1;
		@Key(order = 2)
		public  String[] key2;
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
	
	@Persisting(storeKeys=true, storeAlsoInSuperClasses=true)
	public static class InheritingElement extends SimpleElement {

		public InheritingElement(String key1, String[] key2) {
			super(key1, key2);
		}
	}

	private SimpleElement sut1;
	private InheritingElement sutH;

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

	private void hadAQuery() {
		assertTrue(Memory.INSTANCE.hadAQuery());
	}

	private void hadNoQuery() {
		assertTrue(Memory.INSTANCE.hadNoQuery());
	}
	
	@Test
	public void inexisting() throws DatabaseNotReachedException {
		SimpleElement unknown = new SimpleElement("guhkguilnu", new String [] {"gbuyikgnui", "yuihju"});
		hadNoQuery();
		assertFalse(unknown.existsInStore());
		hadAQuery();
		unknown.activate();
		hadAQuery();
		assertFalse(unknown.existsInStore());
		hadAQuery();
	}

	@Test
	public void getSutTable() {
		assertEquals("SimpleElement", this.sut1.getTable());
		hadNoQuery();
	}

	@Test
	public void storeProperty() throws DatabaseNotReachedException {
		assertEquals("pro1value", ConversionTools.convert(String.class, Memory.INSTANCE.get(
				this.sut1.getTable(), this.sut1.getIdentifier(),
				PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "prop1")));
		Memory.INSTANCE.resetQueries();
		this.sut1.prop1 = "another prop1 value";
		hadNoQuery();
		assertEquals("pro1value", ConversionTools.convert(String.class, Memory.INSTANCE.get(
				this.sut1.getTable(), this.sut1.getIdentifier(),
				PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "prop1")));
		Memory.INSTANCE.resetQueries();
		this.sut1.store();
		hadAQuery();
		assertEquals("another prop1 value", ConversionTools.convert(String.class, Memory.INSTANCE.get(
				this.sut1.getTable(), this.sut1.getIdentifier(),
				PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "prop1")));
	}

	@Test
	public void soreNoKeyInProperties() {
		assertFalse(Memory.INSTANCE.getTable(sut1.getTable()).get(sut1.getIdentifier()).get(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME).containsKey("key1"));
		assertFalse(Memory.INSTANCE.getTable(sut1.getTable()).get(sut1.getIdentifier()).get(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME).containsKey("key2"));
	}

	@Test
	public void soreKeyInProperties() throws DatabaseNotReachedException {
		sutH = new InheritingElement("ik1", new String[]{"ik21", "ik22"});
		sutH.store();
		assertEquals("ik1", ConversionTools.convert(String.class, Memory.INSTANCE.get(
				this.sutH.getTable(), this.sutH.getIdentifier(),
				PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "key1")));
		assertArrayEquals(new String [] {"ik21", "ik22"}, ConversionTools.convert(String[].class, Memory.INSTANCE.get(
				this.sutH.getTable(), this.sutH.getIdentifier(),
				PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "key2")));
		assertFalse(Memory.INSTANCE.getTable(sut1.getTable()).get(sut1.getIdentifier()).get(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME).containsKey("key1"));
		assertFalse(Memory.INSTANCE.getTable(sut1.getTable()).get(sut1.getIdentifier()).get(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME).containsKey("key2"));
	}

	@Test
	public void retreive() throws DatabaseNotReachedException {
		SimpleElement sut2 = new SimpleElement("KEY1", new String[]{"KE", "Y2"});
		hadNoQuery();
		sut2.activate();
		hadAQuery();
		assertFalse(sut2.hasChanged());
		assertEquals("pro1value", sut2.prop1);
		hadNoQuery();
	}

	@Test
	public void getNullProp() {
		assertNull(this.sut1.nullProp);
		hadNoQuery();
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
		hadAQuery();
		assertFalse(Memory.INSTANCE.get(this.sut1.getTable(),
				this.sut1.getIdentifier(),
				PropertyManagement.PROPERTY_COLUMNFAMILY_NAME).containsKey(
				"prop1"));
		Memory.INSTANCE.resetQueries();
		SimpleElement sut2 = new SimpleElement("KEY1", new String[]{"KE", "Y2"});
		hadNoQuery();
		sut2.activate();
		hadAQuery();
		assertEquals(null, sut2.prop1);
		hadNoQuery();
	}

	@Test
	public void changedObject() {
		assertFalse(this.sut1.hasChanged());
		this.sut1.prop2 = false;
		this.sut1.storeProperties();
		assertTrue(this.sut1.hasChanged());
		hadNoQuery();
	}

	@Test
	public void retreiveArrayProperty() throws DatabaseNotReachedException {
		this.sut1.intsProp = new int [] {1,2,3};
		hadNoQuery();
		this.sut1.store();
		hadAQuery();
		SimpleElement sut2 = new SimpleElement("KEY1", new String[]{"KE", "Y2"});
		sut2.activate();
		hadAQuery();
		assertArrayEquals(this.sut1.intsProp, sut2.intsProp);
		hadNoQuery();
	}

	@Test
	public void retreiveByteArrayProperty() throws DatabaseNotReachedException {
		this.sut1.bytesProp = new byte [] {1,2,3};
		hadNoQuery();
		this.sut1.store();
		hadAQuery();
		SimpleElement sut2 = new SimpleElement("KEY1", new String[]{"KE", "Y2"});
		sut2.activate();
		hadAQuery();
		assertArrayEquals(this.sut1.bytesProp, sut2.bytesProp);
		hadNoQuery();
	}

	@Test
	public void retreivePrivateProperty() throws DatabaseNotReachedException {
		this.sut1.setPrivProp("privatevalue");
		hadNoQuery();
		this.sut1.store();
		hadAQuery();
		SimpleElement sut2 = new SimpleElement("KEY1", new String[]{"KE", "Y2"});
		sut2.activate();
		hadAQuery();
		assertEquals("privatevalue", sut2.getPrivProp());
		hadNoQuery();
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
		public  String key = "singleton";
		public Object property;
	}

	@Test(expected = IllegalStateException.class)
	public void invalidProperty() {
		DummyPersister dp = new DummyPersister();
		dp.property = new Object();
	}
	
	@Persisting
	public static class IncrementingElement {
		@Key public  String key;
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
		hadAQuery();
		elt = new IncrementingElement("elt");
		elt.lval = 127l;
		elt.store();
		hadAQuery();
		elt = new IncrementingElement("elt");
		elt.activate();
		hadAQuery();
		assertEquals(250, elt.lval);
		hadNoQuery();
		
		elt.lval += 51;
		elt.store();
		hadAQuery();
		elt = new IncrementingElement("elt");
		elt.activate();
		hadAQuery();
		assertEquals(301, elt.lval);
		hadNoQuery();
		
	}
	@Test
	public void incrementingInt() throws DatabaseNotReachedException {
		IncrementingElement elt = new IncrementingElement("elt");
		elt.ival = 123;
		elt.store();
		hadAQuery();
		elt = new IncrementingElement("elt");
		elt.ival = 127;
		elt.store();
		hadAQuery();
		elt = new IncrementingElement("elt");
		elt.activate();
		hadAQuery();
		assertEquals(250, elt.ival);
		hadNoQuery();
	}
	@Test
	public void incrementingShort() throws DatabaseNotReachedException {
		IncrementingElement elt = new IncrementingElement("elt");
		elt.sval = 123;
		elt.store();
		hadAQuery();
		elt = new IncrementingElement("elt");
		elt.sval = 127;
		elt.store();
		hadAQuery();
		elt = new IncrementingElement("elt");
		elt.activate();
		hadAQuery();
		assertEquals(250, elt.sval);
		hadNoQuery();
	}
	@Test
	public void incrementingByte() throws DatabaseNotReachedException {
		IncrementingElement elt = new IncrementingElement("elt");
		elt.bval = 19;
		elt.store();
		hadAQuery();
		elt = new IncrementingElement("elt");
		elt.bval = 3;
		elt.store();
		hadAQuery();
		elt = new IncrementingElement("elt");
		elt.activate();
		hadAQuery();
		assertEquals(22, elt.bval);
		hadNoQuery();
	}

	@Persisting
	public static class SimpleElementSubclass extends SimpleElement {
		public SimpleElementSubclass(String key1, String[] key2) {
			super(key1, key2);
		}
		
		public String getKey1()	{
			return this.key1;
		}
	}
	
	@Test
	public void inheritance() throws DatabaseNotReachedException {
		SimpleElementSubclass s = new SimpleElementSubclass("ses", new String[]{"KE", "Y2"});
		s.prop1 = "pro2value";
		s.prop2 = false;
		s.store();
		Memory.INSTANCE.resetQueries();
		assertTrue(Memory.INSTANCE.getTable(s.getTable()).containsKey(s.getIdentifier()));
		assertTrue(Memory.INSTANCE.getTable(sut1.getTable()).containsKey(s.getFullIdentifier()));
//		assertTrue(Memory.INSTANCE.getTable(sut1.getTable()).get(s.getFullIdentifier()).containsKey(StorageManagement.CLASS_COLUMN_FAMILY));
//		assertTrue(Memory.INSTANCE.getTable(sut1.getTable()).get(s.getFullIdentifier()).get(StorageManagement.CLASS_COLUMN_FAMILY).containsKey(StorageManagement.CLASS_COLUMN));
//		assertFalse(Memory.INSTANCE.getTable(sut1.getTable()).get(s.getFullIdentifier()).get(StorageManagement.CLASS_COLUMN_FAMILY).containsKey(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
//		assertEquals(s.getClass().getName(), ConversionTools.convert(String.class, Memory.INSTANCE.getTable(sut1.getTable()).get(s.getFullIdentifier()).get(StorageManagement.CLASS_COLUMN_FAMILY).get(StorageManagement.CLASS_COLUMN)));
//		assertFalse(Memory.INSTANCE.getTable(s.getTable()).get(s.getIdentifier()).containsKey(StorageManagement.CLASS_COLUMN_FAMILY));
		assertTrue(Memory.INSTANCE.getTable(s.getTable()).get(s.getIdentifier()).containsKey(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		//Keys are not supposed to be stored as a property in this case
		assertFalse(Memory.INSTANCE.getTable(s.getTable()).get(s.getIdentifier()).get(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME).containsKey("key1"));
		assertEquals(s.prop1, ConversionTools.convert(String.class, Memory.INSTANCE.getTable(s.getTable()).get(s.getIdentifier()).get(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME).get("prop1")));
	}
	
	@Test
	public void multipleActivations() {
		sut1.activateIfNotAlready();
		hadAQuery();
		sut1.activateIfNotAlready();
		assertFalse(Memory.INSTANCE.hadAQuery());
		sut1.activate();
		hadAQuery();
	}
}
