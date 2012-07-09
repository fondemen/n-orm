package com.googlecode.n_orm;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.googlecode.n_orm.cf.CollectionStorageTest;

@RunWith(Suite.class)
@SuiteClasses(value={
		BasicTest.class,
		IncrementsTest.class,
		CollectionStorageTest.class,
		PersistableSearchTest.class,
		InheritanceTest.class,
		ReveresedOrderSearchTest.class,
		EvolutionTest.class,
		FederatedTablesTest.class
		//ImportExportTest.class
})

public class GenericTests {

}
