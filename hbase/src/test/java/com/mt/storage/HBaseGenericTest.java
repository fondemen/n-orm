package com.mt.storage;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses(GenericTests.class)
public class HBaseGenericTest {
	
	@BeforeClass public static void setupStore() {
		StoreTestLauncher.INSTANCE = new HBaseLauncher();
	}
}
