package com.googlecode.n_orm;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.googlecode.n_orm.cf.CollectionStorageTest;
import com.googlecode.n_orm.performance.BasicPerformanceTest;
import com.googlecode.n_orm.performance.PerformanceInserts;

@RunWith(Suite.class)
@SuiteClasses(value={
		// PerformanceInserts.class,
		BasicPerformanceTest.class,
})

public class PerformanceTests {

}
