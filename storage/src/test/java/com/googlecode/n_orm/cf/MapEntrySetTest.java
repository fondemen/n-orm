package com.googlecode.n_orm.cf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.collections.map.AbstractTestMap;
import org.apache.commons.collections.set.AbstractTestSet;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

public class MapEntrySetTest extends AbstractTestSet {
	@Persisting
	public static class Element {
		@Key
		public String key;
		public MapColumnFamily<String, String> elements = new MapColumnFamily<String, String>();
	}

	Element element;
	MapColumnFamily<String, String> aMap;

	public MapEntrySetTest(String name) {
		super(name);
	}

	@Override
	public boolean isNullSupported() {
		return false;
	}

	@Override
	public Map.Entry<String, String>[] getFullNonNullElements() {
		return this.toEntries( new String[] { new String(""), /*new String("One"),*/ "Three",
				"One", "Seven", "Eight", new String("Nine"), "Thirteen", "14",
				"15" } );
	}

	public Map.Entry<String, String>[] getOtherNonNullElements() {
		return this.toEntries( new String[] { "Zero", "0" } );
	}
	
	public Entry<String, String>[] toEntries(String[] vals) {
		Entry[] ret = new Entry[vals.length];
		Map m = new TreeMap<String, String>();
		for (int i = 0; i < vals.length; ++i) {
			m.clear();
			m.put(vals[i], vals[vals.length-i-1]);
			ret[i] = (Entry) m.entrySet().iterator().next();
		}
		return ret;
	}

	@Override
	public Set makeEmptySet() {
		element = new Element();
		aMap = element.elements;
		return aMap.entrySet();
	}
	
	@Override
    public Set makeFullSet() {
		element = new Element();
		aMap = element.elements;
		for (Object elt : getFullElements()) {
			Entry e = (Entry)elt;
			aMap.put((String)e.getKey(), (String)e.getValue());
		}
		return aMap.entrySet();
    }
	
	@Override
    public void testCollectionIteratorRemove() {
		Iterator it = this.makeFullCollection().iterator();
		it.next();
		try {
			it.remove();
			fail(UnsupportedOperationException.class.getName() + " expected" );
		} catch (UnsupportedOperationException x) {}
	}
	
	@Override
    public void testCollectionRetainAll() {
	}

}
