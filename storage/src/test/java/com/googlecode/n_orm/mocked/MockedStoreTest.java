package com.googlecode.n_orm.mocked;

import java.util.Map;
import java.util.Set;

import org.mockito.Mockito;

import com.googlecode.n_orm.Callback;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.Process;
import com.googlecode.n_orm.storeapi.ActionnableStore;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;

public class MockedStoreTest implements ActionnableStore{
	public static final MockedStoreTest INSTANCE = new MockedStoreTest();
	
	private final ActionnableStore mock = Mockito.mock(ActionnableStore.class);

	public ActionnableStore getMock() {
		return mock;
	}
	
	public <AE extends PersistingElement, E extends AE> void process(
			MetaInformation meta, String table, Constraint c,
			Set<String> families, Class<E> element, Process<AE> action,
			Callback callback) throws DatabaseNotReachedException {
		mock.process(meta, table, c, families, element, action, callback);
	}

	public void start() throws DatabaseNotReachedException {
		mock.start();
	}

	public boolean hasTable(String tableName)
			throws DatabaseNotReachedException {
		return mock.hasTable(tableName);
	}

	public void delete(MetaInformation meta, String table, String id)
			throws DatabaseNotReachedException {
		mock.delete(meta, table, id);
	}

	public boolean exists(MetaInformation meta, String table, String row)
			throws DatabaseNotReachedException {
		return mock.exists(meta, table, row);
	}

	public boolean exists(MetaInformation meta, String table, String row,
			String family) throws DatabaseNotReachedException {
		return mock.exists(meta, table, row, family);
	}

	public CloseableKeyIterator get(MetaInformation meta, String table,
			Constraint c, int limit, Set<String> families)
			throws DatabaseNotReachedException {
		return mock.get(meta, table, c, limit, families);
	}

	public byte[] get(MetaInformation meta, String table, String row,
			String family, String key) throws DatabaseNotReachedException {
		return mock.get(meta, table, row, family, key);
	}

	public Map<String, byte[]> get(MetaInformation meta, String table,
			String id, String family) throws DatabaseNotReachedException {
		return mock.get(meta, table, id, family);
	}

	public Map<String, byte[]> get(MetaInformation meta, String table,
			String id, String family, Constraint c)
			throws DatabaseNotReachedException {
		return mock.get(meta, table, id, family, c);
	}

	public ColumnFamilyData get(MetaInformation meta, String table, String id,
			Set<String> families) throws DatabaseNotReachedException {
		return mock.get(meta, table, id, families);
	}

	public void storeChanges(MetaInformation meta, String table, String id,
			ColumnFamilyData changed, Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> increments)
			throws DatabaseNotReachedException {
		mock.storeChanges(meta, table, id, changed, removed, increments);
	}

	public long count(MetaInformation meta, String table, Constraint c)
			throws DatabaseNotReachedException {
		return mock.count(meta, table, c);
	}
	
	
}
