package com.googlecode.n_orm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

public class SerializationTest {
	
	@Persisting
	public static class Element {
		private static final long serialVersionUID = -6920461762920840685L;
		@Key String k1;
		@Key(order=2) String k2;
		int prop;
		Set<Element> elements = null;
	}
	
	public Element serializeDeserialize(Element e) throws IOException, ClassNotFoundException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(e);
		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		ObjectInputStream ois = new ObjectInputStream(bis);
		return (Element)ois.readObject();
	}
	
	public void assertElementEquals(Element expected, Element actual) {
		assertElementEquals(expected, actual, new HashSet<Element>());
	}
	
	public void assertElementEquals(Element expected, Element actual, Set<Element> toBeIgnored) {
		if (!toBeIgnored.add(expected))
			return;
		
		assertEquals(expected, actual);
		assertEquals(expected.k1, actual.k1);
		assertEquals(expected.k2, actual.k2);
		assertEquals(expected.prop, actual.prop);

		assertEquals(expected.elements.size(), actual.elements.size());
		Iterator<Element> sortedExpectedElementsIt = new TreeSet<Element>(expected.elements).iterator();
		Iterator<Element> sortedActualElementsIt = new TreeSet<Element>(actual.elements).iterator();
		while(sortedExpectedElementsIt.hasNext()) {
			assertTrue(sortedActualElementsIt.hasNext());
			Element includedExpected = sortedExpectedElementsIt.next();
			Element includedActual = sortedActualElementsIt.next();
			
			assertElementEquals(includedExpected, includedActual, toBeIgnored);
		}
		assertFalse(sortedActualElementsIt.hasNext());
	}

	@Test
	public void simpleSer() throws IOException, ClassNotFoundException {
		Element e = new Element();
		e.k1 = "k1"; e.k2 = "k2"; e.prop = 3;
		
		Element f = serializeDeserialize(e);
		assertElementEquals(e, f);
		
	}

	@Test
	public void includedSer() throws IOException, ClassNotFoundException {
		Element e1 = new Element();
		e1.k1 = "k11"; e1.k2 = "k12"; e1.prop = 1;
		Element e2 = new Element();
		e2.k1 = "k21"; e2.k2 = "k22"; e2.prop = 2;
		e1.elements.add(e2);
		
		Element f1 = serializeDeserialize(e1);
		assertElementEquals(e1, f1);
	}

	@Test
	public void cyclicSer() throws IOException, ClassNotFoundException {
		Element e1 = new Element();
		e1.k1 = "k11"; e1.k2 = "k12"; e1.prop = 1;
		Element e2 = new Element();
		e2.k1 = "k21"; e2.k2 = "k22"; e2.prop = 2;
		e1.elements.add(e2);
		e2.elements.add(e1);
		
		Element f1 = serializeDeserialize(e1);
		assertElementEquals(e1, f1);
	}

}
