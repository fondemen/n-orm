package com.googlecode.n_orm.cf;

import java.util.Iterator;

@SuppressWarnings("rawtypes")
public class MapKeyEntriesIteratorTest extends MapIteratorTest {

	public MapKeyEntriesIteratorTest(String name) {
		super(name);
	}

	@Override
	public Iterator getIterator() {
		return aMap.entrySet().iterator();
	}

	@Override
	public boolean supportsRemove() {
		return false;
	}

}
