package com.googlecode.n_orm;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.googlecode.n_orm.performance.BasicPerformanceTest;

@RunWith(Suite.class)
@SuiteClasses(value={
		BasicPerformanceTest.class
})

public class PerformanceTests {

}
