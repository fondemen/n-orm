package com.googlecode.n_orm;

import static org.junit.Assert.assertTrue;

import java.util.Properties;


import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.memory.Memory;


public class MemoryStoreTestLauncher extends StoreTestLauncher {
	
	private static Properties properties;

	static {
		properties = new Properties();
		properties.setProperty(StoreSelector.STORE_DRIVERCLASS_PROPERTY,
				Memory.class.getName());
		properties.setProperty(StoreSelector.STORE_DRIVERCLASS_SINGLETON_PROPERTY,
				"INSTANCE");
	}

	@Override
	public Properties prepare(Class<?> testClass) {
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
