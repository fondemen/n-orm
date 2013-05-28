package com.googlecode.n_orm.cf;

import java.util.Set;

import org.apache.commons.collections.set.AbstractTestSet;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

public class SetIntTest extends AbstractTestSet {
	@Persisting
	public static class Element {
		private static final long serialVersionUID = 1982762033333846179L;
		@Key
		public String key;
		public SetColumnFamily<Integer> elements = new SetColumnFamily<Integer>();
	}

	Element element;
	SetColumnFamily<Integer> aSet;

	public SetIntTest(String name) {
		super(name);
	}

	@Override
	public Set<Integer> makeEmptySet() {
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
	public Integer[] getFullNonNullElements() {
		return new Integer[] { 0, 1, 3,
				1, 7, 8, -9, 13, 14,
				15 };
	}

	public Integer[] getOtherNonNullElements() {
		return new Integer[] { 12, -12};
	}

}
