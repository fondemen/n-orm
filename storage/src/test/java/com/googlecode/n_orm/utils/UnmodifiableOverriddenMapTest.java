package com.googlecode.n_orm.utils;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.collections.map.AbstractTestMap;

@SuppressWarnings("rawtypes")
public class UnmodifiableOverriddenMapTest extends AbstractTestMap {

	public UnmodifiableOverriddenMapTest() {
		super(UnmodifiableOverriddenMapTest.class.getSimpleName());
	}

	@Override
	public boolean isAllowDuplicateValues() {
		return true;
	}

	@Override
	public boolean isAllowNullKey() {
		return false;
	}

	@Override
	public boolean isAllowNullValue() {
		return true;
	}

	@Override
	public boolean isPutAddSupported() {
		return false;
	}

	@Override
	public boolean isPutChangeSupported() {
		return false;
	}

	@Override
	public boolean isRemoveSupported() {
		return false;
	}

	@Override
	public boolean isSetValueSupported() {
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map makeFullMap() {
		Map res1 = new HashMap();
		addSampleMappings(res1);
		Map res2 = new HashMap(res1);
		Iterator it = res1.keySet().iterator();
		Object k1 = it.next(), k2 = it.next(), k3 = it.next();
		res1.put(k1, new Object()); // overridden by res2
		res1.remove(k2); // to be found in res2
		res2.remove(k3); // to be found in res1 after failing res2
		UnmodifiableOverridingMap ret = new UnmodifiableOverridingMap();
		ret.override(res1);
		ret.override(res2);
		return ret;
	}

	@Override
	public Map makeEmptyMap() {
		return new UnmodifiableOverridingMap<Object, Object>();
	}

}
