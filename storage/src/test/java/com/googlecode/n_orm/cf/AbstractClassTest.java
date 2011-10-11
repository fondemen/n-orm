package com.googlecode.n_orm.cf;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;

public class AbstractClassTest {
	public abstract static class NonPersistingClass {
		@Key public String key = "";
		public SetColumnFamily<String> set = new SetColumnFamily<String>();
		public MapColumnFamily<String, String> map = new MapColumnFamily<String, String>();
		public Set<String> set2 = new TreeSet<String>();
		public Map<String, String> map2 = new TreeMap<String, String>();
	}
	@Persisting
	public static class PersistingClass extends NonPersistingClass {
	}

	@Test
	public void putAndGet() {
		NonPersistingClass sut = new PersistingClass();
		assertTrue(sut instanceof PersistingElement);
		sut.set.add("addedtoset");
		sut.map.put("addedtomapkey", "addedtomapvalue");
		assertFalse(sut.set.contains("gcfkqc,hfiliu"));
		assertTrue(sut.set.contains("addedtoset"));
		assertFalse(sut.map.containsKey((Object)"hfuisehf,guezgfya"));
		assertTrue(sut.map.containsKey((Object)"addedtomapkey"));
		assertTrue(sut.map.containsValue((Object)"addedtomapvalue"));
		assertEquals("addedtomapvalue", sut.map.get("addedtomapkey"));
		sut.set2.add("addedtoset2");
		sut.map2.put("addedtomap2key", "addedtomap2value");
		assertFalse(sut.set2.contains("gcfkqc,hfiliu"));
		assertTrue(sut.set2.contains("addedtoset2"));
		assertFalse(sut.map2.containsKey((Object)"hfuisehf,guezgfya"));
		assertTrue(sut.map2.containsKey((Object)"addedtomap2key"));
		assertTrue(sut.map2.containsValue((Object)"addedtomap2value"));
		assertEquals("addedtomap2value", sut.map2.get("addedtomap2key"));
	}
}
