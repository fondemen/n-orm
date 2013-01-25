package com.googlecode.n_orm.cf;

import java.util.Iterator;

import org.apache.commons.collections.iterators.AbstractTestIterator;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@SuppressWarnings("rawtypes")
public abstract class MapIteratorTest extends AbstractTestIterator {
	@Persisting
	public static class Element {
		private static final long serialVersionUID = 5940954039477912163L;
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
