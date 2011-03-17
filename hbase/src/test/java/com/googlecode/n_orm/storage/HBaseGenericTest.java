package com.googlecode.n_orm.storage;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.googlecode.n_orm.GenericTests;
import com.googlecode.n_orm.StoreTestLauncher;

@RunWith(Suite.class)
@SuiteClasses(GenericTests.class)
public class HBaseGenericTest {
	
	@BeforeClass public static void setupStore() {
		StoreTestLauncher.INSTANCE = new HBaseLauncher();
	}
}
