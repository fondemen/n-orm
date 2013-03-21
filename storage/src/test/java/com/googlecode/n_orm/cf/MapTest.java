package com.googlecode.n_orm.cf;

import java.util.Map;

import org.apache.commons.collections.map.AbstractTestMap;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

public class MapTest extends AbstractTestMap {
	@Persisting
	public static class Element {
		private static final long serialVersionUID = 4414752177673396715L;
		@Key
		public String key;
		public MapColumnFamily<String, String> elements = new MapColumnFamily<String, String>();
	}

	Element element;
	MapColumnFamily<String, String> aMap;

	public MapTest(String name) {
		super(name);
	}

	@Override
	public boolean isAllowNullKey() {
		return false;
	}

	@Override
	public boolean isAllowNullValue() {
		return false;
	}

	@Override
	public Map<String, String> makeEmptyMap() {
		element = new Element();
		aMap = element.elements;
		return aMap;
	}

}
