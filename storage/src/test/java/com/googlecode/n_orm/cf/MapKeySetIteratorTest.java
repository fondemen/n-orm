package com.googlecode.n_orm.cf;

import java.util.Iterator;

public class MapKeySetIteratorTest extends MapIteratorTest {

	public MapKeySetIteratorTest(String name) {
		super(name);
	}

	@Override
	public Iterator getIterator() {
		return aMap.keySet().iterator();
	}

	@Override
	public boolean supportsRemove() {
		return false;
	}

}
