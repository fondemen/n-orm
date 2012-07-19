package com.googlecode.n_orm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

import com.googlecode.n_orm.mocked.ElementInFederatedMockedStore;
import com.googlecode.n_orm.mocked.MockedStoreTest;
import com.googlecode.n_orm.storeapi.MetaInformation;

public class FederatedTableProcessTest {

	@Test
	public void remoteForeach() throws DatabaseNotReachedException, InstantiationException, IllegalAccessException {
		reset(MockedStoreTest.INSTANCE.getMock());
		Map<String, byte[]> alts = new TreeMap<String, byte[]>();
		alts.put("", null);
		alts.put("post1", null);
		alts.put("post2", null);
		when(MockedStoreTest.INSTANCE.getMock().get(null, FederatedTableManagement.FEDERATED_META_TABLE, PersistingMixin.getInstance().getTable(ElementInFederatedMockedStore.class), FederatedTableManagement.FEDERATED_META_COLUMN_FAMILY)).thenReturn(alts);
		when(MockedStoreTest.INSTANCE.getMock().hasTable("t")).thenReturn(true);
		when(MockedStoreTest.INSTANCE.getMock().hasTable("tpost1")).thenReturn(true);
		when(MockedStoreTest.INSTANCE.getMock().hasTable("tpost2")).thenReturn(true);
		
		Process<ElementInFederatedMockedStore> process = new Process<ElementInFederatedMockedStore>() {
			
			@Override
			public void process(ElementInFederatedMockedStore element) throws Throwable {				
			}
		};
		Set<String> fams = new TreeSet<String>();
		Map<String, Field> famsFields = new TreeMap<String, Field>();
		
		StorageManagement.findElements().ofClass(ElementInFederatedMockedStore.class).remoteForEach(process, null, 10000, 10000);
		
		verify(MockedStoreTest.INSTANCE.getMock()).process(new MetaInformation().forClass(ElementInFederatedMockedStore.class).withColumnFamilies(famsFields).withPostfixedTable("t", ""     ), "t"     , null, fams, ElementInFederatedMockedStore.class, process, null);
		verify(MockedStoreTest.INSTANCE.getMock()).process(new MetaInformation().forClass(ElementInFederatedMockedStore.class).withColumnFamilies(famsFields).withPostfixedTable("t", "post1"), "tpost1", null, fams, ElementInFederatedMockedStore.class, process, null);
		verify(MockedStoreTest.INSTANCE.getMock()).process(new MetaInformation().forClass(ElementInFederatedMockedStore.class).withColumnFamilies(famsFields).withPostfixedTable("t", "post2"), "tpost2", null, fams, ElementInFederatedMockedStore.class, process, null);
		
	}

}
