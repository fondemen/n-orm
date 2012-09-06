package com.googlecode.n_orm.hbase;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.googlecode.n_orm.PerformanceTests;
import com.googlecode.n_orm.StoreTestLauncher;

@RunWith(Suite.class)
@SuiteClasses(PerformanceTests.class)
public class HBasePerformanceTest {

	@BeforeClass public static void setupStore() {
		StoreTestLauncher.INSTANCE = new HBaseLauncher();
	}
	
}


