package com.googlecode.n_orm.cf;

import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.keyvalue.AbstractTestMapEntry;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@SuppressWarnings("rawtypes")
public class EntryTest extends AbstractTestMapEntry {
	@Persisting
	public static class Element {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2337859385628053298L;
		@Key
		public String key;
		public MapColumnFamily<String, String> elements = new MapColumnFamily<String, String>();
	}

	Element element;
	MapColumnFamily<String, String> aMap;
	Set<Entry<String, String>> aSet;

	public EntryTest(String name) {
		super(name);
	}

	@Override
	public Entry makeMapEntry(Object key, Object value) {
		if (element == null) {
			element = new Element();
			element.key = "testelt";
			aMap = element.elements;
			aSet = aMap.entrySet();
		}
		aMap.put((String)key, (String)value);
		for (Entry<String, String> entry : aSet) {
			if (entry.getKey().equals(key))
				return entry;
		}
		fail("could not create entry " + key + '=' + value);
		return null;
	}

	@Override
	public void testConstructors() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Entry makeMapEntry() {
		return makeMapEntry("key", "value");
	}
	
	@Override
    public Map.Entry makeKnownMapEntry() {
        return makeKnownMapEntry("key", "value");
    }

	@SuppressWarnings("unchecked")
	@Override
	public void testAccessorsAndMutators() {
        Map.Entry entry = makeMapEntry(key, value);

        assertTrue(entry.getKey() == key);

        entry.setValue(value);
        assertTrue(entry.getValue() == value);
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
            assertTrue(entry.getKey().equals("key") && entry.getValue().equals("value"));
        }
    }

}
