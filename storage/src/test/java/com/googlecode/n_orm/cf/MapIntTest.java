package com.googlecode.n_orm.cf;

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.map.AbstractTestMap;
import org.apache.commons.collections.set.AbstractTestSet;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

public class MapIntTest extends AbstractTestMap {
	@Persisting
	public static class Element {
		@Key
		public String key;
		public MapColumnFamily<Integer, Integer> elements = new MapColumnFamily<Integer, Integer>();
	}

	Element element;
	MapColumnFamily<Integer, Integer> aMap;

	public MapIntTest(String name) {
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
	public Map<Integer, Integer> makeEmptyMap() {
		element = new Element();
		aMap = element.elements;
		return aMap;
	}
	
    public Object[] getSampleKeys() {
        return new Object[] {
            1,9,27,-1,0,1000,-235674
        };
    }
	
    public Object[] getSampleValues() {
        return new Object[] {
            12,24,36,5,-456,0,998
        };
    }
    
    public Object[] getNewSampleValues() {
        return new Object[] {
            0,1024,-99,18,54,6,77
        };
    }


    public Object[] getOtherKeys() {
        return new Object[] {
                92,68,-84,111,123456789,-123,888
        };
    }

}
