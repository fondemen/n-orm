package com.googlecode.n_orm.cf;

import java.util.Set;

import org.apache.commons.collections.set.AbstractTestSet;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

public class SetTest extends AbstractTestSet {
	@Persisting
	public static class Element {
		@Key
		public String key;
		public SetColumnFamily<String> elements = new SetColumnFamily<String>();
	}

	Element element;
	SetColumnFamily<String> aSet;

	public SetTest(String name) {
		super(name);
	}

	@Override
	public Set<String> makeEmptySet() {
		element = new Element();
		element.key = "testelt";
		aSet = element.elements;
		return aSet;
	}

	@Override
	public boolean isNullSupported() {
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

}
