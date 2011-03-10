package org.norm.storage;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.norm.GenericTests;
import org.norm.StoreTestLauncher;

@RunWith(Suite.class)
@SuiteClasses(GenericTests.class)
public class HBaseGenericTest {
	
	@BeforeClass public static void setupStore() {
		StoreTestLauncher.INSTANCE = new HBaseLauncher();
	}
}
