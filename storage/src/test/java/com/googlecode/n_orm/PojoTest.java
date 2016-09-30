package com.googlecode.n_orm;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.cf.MapColumnFamily;
import com.googlecode.n_orm.cf.SetColumnFamily;


public class PojoTest {
	private SimpleElement sut;
	private SimpleElement inner1;
	private SimpleElement inner2;

	@Persisting
	public static class SimpleElement {
		private static final long serialVersionUID = -6353654986364855413L;
		@Key public String key;
		Set<SimpleElement> elementsSet = new TreeSet<PojoTest.SimpleElement>();
		Map<String, SimpleElement> elementsMap = new TreeMap<String, PojoTest.SimpleElement>();
	}

	@Before
	public void createSut() {
		sut = new SimpleElement();
		sut.key = "key";
		inner1 = new SimpleElement(); inner1.key = "inner1";
		inner2 = new SimpleElement(); inner2.key = "inner2";
		sut.elementsSet.add(inner1); sut.elementsSet.add(inner2);
		sut.elementsMap.put("i1", inner1); sut.elementsMap.put("i2", inner2);
	}
	
	@Test
	public void comparePojoToColumnFamilies() {
		
		sut.updateFromPOJO();

		SetColumnFamily<?> elementsSetCf = (SetColumnFamily<?>) sut.getColumnFamily(sut.elementsSet);
		MapColumnFamily<?,?> elementsMapCf = (MapColumnFamily<?, ?>) sut.getColumnFamily(sut.elementsMap);
		

		assertNotNull(sut.elementsSet);
		assertNotNull(sut.elementsMap);
		assertFalse(sut.elementsSet instanceof SetColumnFamily<?>);
		assertFalse(sut.elementsMap instanceof MapColumnFamily<?,?>);
		
		assertNotNull(elementsSetCf);
		assertNotNull(elementsMapCf);
		
		assertEquals(2, sut.elementsSet.size());
		assertTrue(elementsSetCf.contains(inner1));
		assertTrue(elementsSetCf.contains(inner2));
		
		assertEquals(2, sut.elementsMap.size());
		assertEquals(inner1, elementsMapCf.get("i1"));
		assertEquals(inner2, elementsMapCf.get("i2"));
	}
	
	@Test
	public void serialization() throws IOException, ClassNotFoundException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream oo = new ObjectOutputStream(out);
		oo.writeObject(sut);
		oo.close();
		
		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		ObjectInputStream oi = new ObjectInputStream(in);
		SimpleElement sut2 = (SimpleElement) oi.readObject();
		
		assertEquals(2, sut2.elementsSet.size());
		assertTrue(sut2.elementsSet.contains(inner1));
		assertTrue(sut2.elementsSet.contains(inner2));
		
		assertEquals(2, sut2.elementsMap.size());
		assertEquals(inner1, sut2.elementsMap.get("i1"));
		assertEquals(inner2, sut2.elementsMap.get("i2"));
		
	}
	
	@Test
	public void fromPojoAfterSerialization() throws IOException, ClassNotFoundException {
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream oo = new ObjectOutputStream(out);
		oo.writeObject(sut);
		oo.close();
		
		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		ObjectInputStream oi = new ObjectInputStream(in);
		SimpleElement sut2 = (SimpleElement) oi.readObject();
		
		assertNotNull(sut2.elementsSet);
		assertNotNull(sut2.elementsMap);
		
		assertEquals(2, sut.elementsSet.size());
		assertTrue(sut2.elementsSet.contains(inner1));
		assertTrue(sut2.elementsSet.contains(inner2));
		
		assertEquals(2, sut2.elementsMap.size());
		assertEquals(inner1, sut2.elementsMap.get("i1"));
		assertEquals(inner2, sut2.elementsMap.get("i2"));
		
	}
	
	@Test
	public void change() {
		sut.store();
		SimpleElement inner3 = new SimpleElement(); inner3.key = "inner3";
		sut.elementsSet.add(inner1); //Not changed
		sut.elementsSet.remove(inner2);
		sut.elementsSet.add(inner3); //New
		
		sut.elementsMap.put("i1", inner1);
		sut.elementsMap.remove("i2");
		sut.elementsMap.put("i3", inner3);
		
		assertTrue(sut.hasChanged());
		
		assertNotNull(sut.elementsSet);
		assertNotNull(sut.elementsMap);
		
		assertEquals(2, sut.elementsSet.size());
		assertTrue(sut.elementsSet.contains(inner1));
		assertFalse(sut.elementsSet.contains(inner2));
		assertTrue(sut.elementsSet.contains(inner3));
		
		assertEquals(2, sut.elementsMap.size());
		assertEquals(inner1, sut.elementsMap.get("i1"));
		assertNull(sut.elementsMap.get("i2"));
		assertEquals(inner3, sut.elementsMap.get("i3"));
	}
	
	/**
	 * Don't store this class !!
	 */
	@Persisting
	public static class DummyClass {
		private static final long serialVersionUID = 9192963374136426936L;
		@Key private String key;
		private String value;
		private Map<String, String> cf = null;
		
		private DummyClass(String key) {
			this.key = key;
			this.addPersistingElementListener(new PersistingElementListener() {
				
				@Override
				public void stored(PersistingElement pe) {
					fail("Do not save this class");
				}
				
				@Override
				public void storeInvoked(PersistingElement pe) {
					fail("Do not save this class");
				}
				
				@Override
				public void activated(PersistingElement pe, Set<ColumnFamily<?>> activatedColumnFamilies) {
				}
				
				@Override
				public void activateInvoked(PersistingElement pe, Set<ColumnFamily<?>> columnFamiliesToBeActivated) {
				}
			});
		}
	}
	
	@Test
	public void activatingUnknownObject() {
		DummyClass sut = new DummyClass("key");
		sut.value = "";
		sut.activate();
		assertNull(sut.value);
	}
	
	@Test
	public void activatingUnknownObjectCF() {
		DummyClass sut = new DummyClass("key");
		sut.cf.put("key", "value");
		sut.activateColumnFamily("cf");
		assertTrue(sut.cf.isEmpty());
	}
	
	@Test
	public void activatingUnknownObjectCFKey() {
		DummyClass sut = new DummyClass("key");
		sut.cf.put("key", "value");
		Object res = sut.getColumnFamily(sut.cf).getFromStore("key");
		assertNull(res);
	}
	
	@Test
	public void activatingUnknownObjectCFKeyWithConstraint() {
		DummyClass sut = new DummyClass("key");
		sut.cf.put("key", "value");
		sut.activateColumnFamily("cf", "a", "Z");
		assertTrue(sut.cf.isEmpty());
	}
	
	@Test
	public void searchingUnknownObject() {
		DummyClass res = StorageManagement.findElements().ofClass(DummyClass.class).andActivateAllFamilies().any();
		assertNull(res);
	}
}
