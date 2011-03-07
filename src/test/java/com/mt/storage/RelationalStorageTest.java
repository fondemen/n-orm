package com.mt.storage;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;

import com.mt.storage.conversion.ConversionTools;
import com.mt.storage.memory.Memory;


public class RelationalStorageTest {

	private static final String ke = KeyManagement.KEY_END_SEPARATOR;
	private static final String ks = KeyManagement.KEY_SEPARATOR;
	
	@After public void resetStorage() {
		Memory.INSTANCE.reset();
	}
	
	public static class SimpleElement {
		@Key public String prop1;
		public SimpleElement(String prop1) {
			super();
			this.prop1 = prop1;
		} 
	}

	@Test public void getFromId() {
		SimpleElement sut = ConversionTools.convert(SimpleElement.class, ("prop1val" + ke).getBytes());
		assertEquals("prop1val", sut.prop1);
		assertEquals("prop1val" + ke, ConversionTools.convertToString(sut, SimpleElement.class));
	}
	
	public static class NotOnlyKeysElement {
		@Key public String prop1;
		public String prop3;
		public NotOnlyKeysElement(String prop1) {
			super();
			this.prop1 = prop1;
		} 
	}
	@Test(expected=IllegalArgumentException.class) public void notOnlyKeysElement() {
		ConversionTools.convert(NotOnlyKeysElement.class, "prop1val".getBytes());
	}
	
	public static class Composed1Element {
		@Key private  SimpleElement key;
		public Composed1Element(SimpleElement key) {
			super();
			this.key = key;
		}
		
		public SimpleElement getKey() {
			return this.key;
		}
	}
	@Test public void composition() {
		Composed1Element sut = ConversionTools.convert(Composed1Element.class, ("prop1val" + ke + ke).getBytes());
		assertEquals("prop1val", sut.key.prop1);
		assertEquals("prop1val" + ke + ke, ConversionTools.convertToString(sut, Composed1Element.class));
	}
	
	public static class Composed2Elements {
		@Key private  SimpleElement key1;
		@Key(order=2) private  Composed1Element key2;
		public Composed2Elements(SimpleElement key1, Composed1Element key2) {
			this.key1 = key1;
			this.key2 = key2;
		}
		public SimpleElement getKey1() {
			return key1;
		}
		public Composed1Element getKey2() {
			return key2;
		}
	}
	@Test public void doubleComposition() {
		Composed2Elements sut = ConversionTools.convert(Composed2Elements.class, ("1prop1val" + ke + ks + "2prop1val" + ke + ke + ke).getBytes());
		assertEquals("1prop1val", sut.key1.prop1);
		assertEquals("2prop1val", sut.key2.key.prop1);
		assertEquals("1prop1val" + ke + ks + "2prop1val" + ke + ke + ke, ConversionTools.convertToString(sut, Composed2Elements.class));
	}
	
	public static class Composed3Elements {
		@Key private  Composed2Elements key1;
		@Key(order=2) private  Composed2Elements key2;
		public Composed3Elements(Composed2Elements key1, Composed2Elements key2) {
			this.key1 = key1;
			this.key2 = key2;
		}
		public Composed2Elements getKey1() {
			return key1;
		}
		public Composed2Elements getKey2() {
			return key2;
		}
	}
	@Test public void tripleComposition() {
		Composed3Elements sut = ConversionTools.convert(Composed3Elements.class, ("11prop1val" + ke + ks + "12prop1val" + ke + "" + ke + "" + ke + ks + "21prop1val" + ke + ks + "22prop1val" + ke + ke + ke + ke).getBytes());
		assertEquals("11prop1val", sut.key1.key1.prop1);
		assertEquals("12prop1val", sut.key1.key2.key.prop1);
		assertEquals("21prop1val", sut.key2.key1.prop1);
		assertEquals("22prop1val", sut.key2.key2.key.prop1);
		assertEquals("11prop1val" + ke + ks + "12prop1val" + ke + "" + ke + "" + ke + ks + "21prop1val" + ke + ks + "22prop1val" + ke + ke + ke + ke, ConversionTools.convertToString(sut, Composed3Elements.class));
	}
	
	@Persisting(table="PC") public static class PersistingComposed {
		@Key public  String key;
		public Composed3Elements value;
		public PersistingComposed(String key) {
			super();
			this.key = key;
		}
	}
	@Test public void persistComposed() throws DatabaseNotReachedException {
		Composed3Elements val = new Composed3Elements(new Composed2Elements(new SimpleElement("1"), new Composed1Element(new SimpleElement("2"))), new Composed2Elements(new SimpleElement("3"), new Composed1Element(new SimpleElement("4"))));		
		PersistingComposed sut = new PersistingComposed("key");
		sut.value = val;
		sut.store();
		assertEquals("1" + ke + ks + "2" + ke + ke + ke + ks + "3" + ke + ks + "4" + ke + ke + ke + ke, ConversionTools.convert(String.class, Memory.INSTANCE.get("PC", "key" + ke, PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "value")));
		PersistingComposed sut2 = new PersistingComposed("key");
		sut2.activate();
		assertEquals("1", sut2.value.key1.key1.prop1);
		assertEquals("2", sut2.value.key1.key2.key.prop1);
		assertEquals("3", sut2.value.key2.key1.prop1);
		assertEquals("4", sut2.value.key2.key2.key.prop1);
	}
	
	@Persisting(table="Inside") public static class PersistingInside {
		@Key public  String key;
		public String val;
		public PersistingInside(String key) {
			super();
			this.key = key;
		}
	}
	@Persisting(table="Outside") public static class PersistingOutside {
		@Key public  String key;
		public PersistingInside val;
		public PersistingOutside(String key) {
			super();
			this.key = key;
		}
	}
	
	@Test public void foreignKey() throws DatabaseNotReachedException {
		PersistingInside in = new PersistingInside("inside");
		in.val = "value";
		PersistingOutside out = new PersistingOutside("outside");
		out.val = in;
		out.store(); //No need to store in ; should be triggered automatically.
		assertEquals("inside" + ke, ConversionTools.convert(String.class, Memory.INSTANCE.get("Outside", "outside" + ke, PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "val")));
		assertEquals("value", ConversionTools.convert(String.class, Memory.INSTANCE.get("Inside", "inside" + ke, PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "val")));
		
		PersistingOutside out2 = new PersistingOutside("outside");
		out2.activate();
		assertNotNull(out2.val);
		assertEquals("inside", out2.val.key);
		assertEquals("value", out2.val.val);
	}
	
	@Test public void foreignKeyWhichHasNotChanged() throws DatabaseNotReachedException {
		PersistingInside in = new PersistingInside("inside");
		in.val = "value";
		in.store();
		Memory.INSTANCE.reset(); //Makes in believe that it already exists while memory forgets
		PersistingOutside out = new PersistingOutside("outside");
		out.val = in;
		out.store(); //Should not store anything
		assertEquals("inside" + ke, ConversionTools.convert(String.class, Memory.INSTANCE.get("Outside", "outside" + ke, PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, "val")));
		assertFalse(Memory.INSTANCE.getTable("Inside").containsKey("inside" + ke)); //memory was reseted ; in was not stored for it thought it was not changed and in store
	}
	
	@Persisting(table="Outside") public static class PersistingOutsideExplicit {
		@Key public  String key;
		@ExplicitActivation public PersistingInside val;
		public PersistingOutsideExplicit(String key) {
			super();
			this.key = key;
		}
	}
	@Test public void notExplicitForeignKey() throws DatabaseNotReachedException {
		PersistingInside in = new PersistingInside("inside");
		in.val = "value";
		PersistingOutsideExplicit out = new PersistingOutsideExplicit("outside");
		out.val = in;
		out.store();
		
		assertFalse(Memory.INSTANCE.getTable("Inside").containsKey("inside" + ke));
		
		in.store();
		assertTrue(Memory.INSTANCE.getTable("Inside").containsKey("inside" + ke));
		
		PersistingOutsideExplicit out2 = new PersistingOutsideExplicit("outside");
		out2.activate();
		assertEquals("inside", out2.val.key);
		assertNull(out2.val.val);
		
		out2.val.activate();
		assertEquals("value", out2.val.val);
	}
}
