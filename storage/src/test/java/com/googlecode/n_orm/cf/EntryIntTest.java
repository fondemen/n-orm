package com.googlecode.n_orm.cf;

import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.keyvalue.AbstractTestMapEntry;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@SuppressWarnings("rawtypes")
public class EntryIntTest extends AbstractTestMapEntry {
	@Persisting
	public static class Element {
		private static final long serialVersionUID = -5973440370958521359L;
		@Key
		public Integer key;
		public MapColumnFamily<Integer, Integer> elements = new MapColumnFamily<Integer, Integer>();
	}

	Element element;
	MapColumnFamily<Integer, Integer> aMap;
	Set<Entry<Integer, Integer>> aSet;

	public EntryIntTest(String name) {
		super(name);
	}

	@Override
	public Entry makeMapEntry(Object key, Object value) {
		if (element == null) {
			element = new Element();
			element.key = 1231;
			aMap = element.elements;
			aSet = aMap.entrySet();
		}
		if (key == null || !(key instanceof Integer))
			key = 1231;
		if (value == null || !(value instanceof Integer))
			value = 246864468;
		aMap.put((Integer)key, (Integer)value);
		for (Entry<Integer, Integer> entry : aSet) {
			if (entry.getKey().equals(key))
				return entry;
		}
		fail("could not create entry " + key + '=' + value);
		return null;
	}

	@Override
	public Entry makeKnownMapEntry(Object key, Object value) {
		if (key == null || !(key instanceof Integer))
			key = 1231;
		if (value == null || !(value instanceof Integer))
			value = 246864468;
		return super.makeKnownMapEntry(key, value);
	}

	@Override
	public void testConstructors() {
		// TODO Auto-generated method stub
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void testAccessorsAndMutators() {
        Map.Entry entry = makeMapEntry(1231, 246864468);

        assertTrue(entry.getKey().equals(1231));

        entry.setValue(246864468);
        assertTrue(entry.getValue().equals(246864468));
    }
	
	@SuppressWarnings("unchecked")
	@Override
    public void testSelfReferenceHandling() {
        // test that #setValue does not permit
        //  the MapEntry to contain itself (and thus cause infinite recursion
        //  in #hashCode and #toString)

        Map.Entry entry = makeMapEntry();

        try {
            entry.setValue(entry);
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            // expected to happen...

            // check that the KVP's state has not changed
            assertTrue(entry.getKey().equals(1231) && entry.getValue().equals(246864468));
        }
    }

}
