package com.googlecode.n_orm.cf;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.map.AbstractTestMap;
import org.apache.commons.collections.set.AbstractTestSet;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

public class MapKeySetIntTest extends AbstractTestSet {
	@Persisting
	public static class Element {
		@Key
		public String key;
		public MapColumnFamily<Integer, Integer> elements = new MapColumnFamily<Integer, Integer>();
	}

	Element element;
	MapColumnFamily<Integer, Integer> aMap;

	public MapKeySetIntTest(String name) {
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
	public Integer[] getFullNonNullElements() {
		return new Integer[] { 0, 1, 3,
				1, 7, 8, -9, 13, 14,
				15 };
	}

	public Integer[] getOtherNonNullElements() {
		return new Integer[] { 12, -12};
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
			aMap.put((Integer)elt, ((Integer)elt)<<3);
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
