package com.googlecode.n_orm.cf;

import java.util.Iterator;
import java.util.Set;

import org.apache.commons.collections.set.AbstractTestSet;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@SuppressWarnings("rawtypes")
public class MapKeySetTest extends AbstractTestSet {
	@Persisting
	public static class Element {
		private static final long serialVersionUID = -3665268168520816480L;
		@Key
		public String key;
		public MapColumnFamily<String, String> elements = new MapColumnFamily<String, String>();
	}

	Element element;
	MapColumnFamily<String, String> aMap;

	public MapKeySetTest(String name) {
		super(name);
	}

	@Override
	public boolean isNullSupported() {
		return false;
	}

	@Override
	public boolean isAddSupported() {
		return false;
	}

	@Override
	public String[] getFullNonNullElements() {
		return new String[] { new String(""), new String("One"), "Three",
				"One", "Seven", "Eight", new String("Nine"), "Thirteen", "14",
				"15" };
	}

	public String[] getOtherNonNullElements() {
		return new String[] { "Zero", "0" };
	}

	@Override
	public Set makeEmptySet() {
		element = new Element();
		aMap = element.elements;
		return aMap.keySet();
	}
	
	@Override
    public Set makeFullSet() {
		makeEmptySet();
		for (Object elt : getFullElements()) {
			aMap.put((String)elt, (String)elt+"_val");
		}
		return aMap.keySet();
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

}
