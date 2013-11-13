package com.googlecode.n_orm;

import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.TreeMap;


import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.memory.Memory;


public class MemoryStoreTestLauncher extends StoreTestLauncher {
	
	private static Map<String, Object> properties;

	static {
		properties = new TreeMap<String, Object>();
		properties.put(StoreSelector.STORE_DRIVERCLASS_PROPERTY,
				Memory.class.getName());
		properties.put(StoreSelector.STORE_DRIVERCLASS_SINGLETON_PROPERTY,
				"INSTANCE");
	}

	@Override
	public Map<String, Object> prepare(Class<?> testClass) {
		MemoryStoreTestLauncher.registerStorePropertiesForInnerClasses(
				this.getClass(), properties);
		return properties;
	}

	public void assertHadAQuery() {
		assertTrue(Memory.INSTANCE.hadAQuery());
	}

	public void assertHadNoQuery() {
		assertTrue(Memory.INSTANCE.hadNoQuery());
	}

	public void resetQueryCount() {
		Memory.INSTANCE.resetQueries();
	}
}
