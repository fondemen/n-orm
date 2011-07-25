package com.googlecode.n_orm.cf;

import java.util.Iterator;
import java.util.Set;

import org.apache.commons.collections.iterators.AbstractTestIterator;
import org.apache.commons.collections.set.AbstractTestSet;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

public abstract class MapIteratorTest extends AbstractTestIterator {
	@Persisting
	public static class Element {
		@Key
		public String key;
		public MapColumnFamily<String, String> elements = new MapColumnFamily<String, String>();
	}

	Element element;
	MapColumnFamily<String, String> aMap;

	public MapIteratorTest(String name) {
		super(name);
	}

	public String[] getFullNonNullElements() {
		return new String[] { new String(""), new String("One"), "Three",
				"One", "Seven", "Eight", new String("Nine"), "Thirteen", "14",
				"15" };
	}
	
	public abstract Iterator getIterator();

	@Override
	public Iterator makeEmptyIterator() {
		element = new Element();
		element.key = "testelt";
		aMap = element.elements;
		return this.getIterator();
	}

	@Override
	public Iterator makeFullIterator() {
		element = new Element();
		element.key = "testelt";
		aMap = element.elements;
		for (String s : getFullNonNullElements()) {
			element.elements.put(s+"_key", s+"_value");
		}
		return this.getIterator();
	}

}
