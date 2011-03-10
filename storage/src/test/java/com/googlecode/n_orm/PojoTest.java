package com.googlecode.n_orm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.Indexed;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.cf.MapColumnFamily;
import com.googlecode.n_orm.cf.SetColumnFamily;


import static org.junit.Assert.*;


public class PojoTest {
	private SimpleElement sut;
	private SimpleElement inner1;
	private SimpleElement inner2;

	@Persisting
	public static class SimpleElement {
		private static final long serialVersionUID = -6353654986364855413L;
		@Key public String key;
		@Indexed(field="key") Set<SimpleElement> elementsSet = null;
		Map<String, SimpleElement> elementsMap = null;
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
	public void toPojo() {
		assertNotNull(sut.elementsSet);
		assertNotNull(sut.elementsMap);
		assertTrue(sut.elementsSet instanceof SetColumnFamily<?>);
		assertTrue(sut.elementsMap instanceof MapColumnFamily<?,?>);
		
		sut.setPOJO(true);
		assertNotNull(sut.elementsSet);
		assertNotNull(sut.elementsMap);
		assertFalse(sut.elementsSet instanceof SetColumnFamily<?>);
		assertFalse(sut.elementsMap instanceof MapColumnFamily<?,?>);
		
		assertEquals(2, sut.elementsSet.size());
		assertTrue(sut.elementsSet.contains(inner1));
		assertTrue(sut.elementsSet.contains(inner2));
		
		assertEquals(2, sut.elementsMap.size());
		assertEquals(inner1, sut.elementsMap.get("i1"));
		assertEquals(inner2, sut.elementsMap.get("i2"));
	}
	
	@Test
	public void fromPojo() {
		sut.setPOJO(true);
		sut.setPOJO(false);
		
		assertNotNull(sut.elementsSet);
		assertNotNull(sut.elementsMap);
		assertTrue(sut.elementsSet instanceof SetColumnFamily<?>);
		assertTrue(sut.elementsMap instanceof MapColumnFamily<?,?>);
		
		assertEquals(2, sut.elementsSet.size());
		assertTrue(sut.elementsSet.contains(inner1));
		assertTrue(sut.elementsSet.contains(inner2));
		
		assertEquals(2, sut.elementsMap.size());
		assertEquals(inner1, sut.elementsMap.get("i1"));
		assertEquals(inner2, sut.elementsMap.get("i2"));
	}
	
	@Test
	public void fromEmptyPojo() {
		sut.setPOJO(true);
		sut.clearColumnFamilies();
		sut.setPOJO(false);
		
		assertNotNull(sut.elementsSet);
		assertNotNull(sut.elementsMap);
		assertTrue(sut.elementsSet instanceof SetColumnFamily<?>);
		assertTrue(sut.elementsMap instanceof MapColumnFamily<?,?>);
		
		assertEquals(2, sut.elementsSet.size());
		assertTrue(sut.elementsSet.contains(inner1));
		assertTrue(sut.elementsSet.contains(inner2));
		
		assertEquals(2, sut.elementsMap.size());
		assertEquals(inner1, sut.elementsMap.get("i1"));
		assertEquals(inner2, sut.elementsMap.get("i2"));
	}
	
	@Test
	public void serialization() throws IOException, ClassNotFoundException {
		sut.setPOJO(true);
		
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
		sut.setPOJO(true);
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream oo = new ObjectOutputStream(out);
		oo.writeObject(sut);
		oo.close();
		
		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		ObjectInputStream oi = new ObjectInputStream(in);
		SimpleElement sut2 = (SimpleElement) oi.readObject();
		
		sut2.setPOJO(false);
		
		assertNotNull(sut2.elementsSet);
		assertNotNull(sut2.elementsMap);
		assertTrue(sut2.elementsSet instanceof SetColumnFamily<?>);
		assertTrue(sut2.elementsMap instanceof MapColumnFamily<?,?>);
		
		assertEquals(2, sut.elementsSet.size());
		assertTrue(sut2.elementsSet.contains(inner1));
		assertTrue(sut2.elementsSet.contains(inner2));
		
		assertEquals(2, sut2.elementsMap.size());
		assertEquals(inner1, sut2.elementsMap.get("i1"));
		assertEquals(inner2, sut2.elementsMap.get("i2"));
		
	}
}
