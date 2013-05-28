package com.googlecode.n_orm.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.map.AbstractTestMap;

@SuppressWarnings("rawtypes")
public class UnmodifiableOverridingMapTest extends AbstractTestMap {

	public UnmodifiableOverridingMapTest() {
		super(UnmodifiableOverridingMapTest.class.getSimpleName());
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
		Map res = new HashMap();
		addSampleMappings(res);
		UnmodifiableOverridingMap ret = new UnmodifiableOverridingMap();
		ret.override(res);
		return ret;
	}

	@Override
	public Map makeEmptyMap() {
		return new UnmodifiableOverridingMap<Object, Object>();
	}

}
