package com.googlecode.n_orm;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses(value={
		BasicTest.class,
		CollectionStorageTest.class,
		PersistableSearchTest.class,
		InheritanceTest.class
})

public class GenericTests {

}
