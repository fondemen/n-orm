package com.googlecode.n_orm.storeapi;

import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;

/**
 *	A store that delegates all of its requests to another one.
 */
public class DelegatingStore implements Store {

	private final Store actualStore;

	public DelegatingStore(Store actualStore) {
		this.actualStore = actualStore;
	}
	
	/**
	 * Returns the {@link Store} to which requests will be delegated to.
	 */
	public Store getActualStore() {
		return this.actualStore;
	}
	
	/**
	 * Finds the {@link Store} to which requests will be delegated to in the delegation chains of the given class.
	 */
	public <T extends Store> T getActualStore(Class<T>clazz) {
		Store ret = this;
		while (true) {
			if (ret == null)
				return null;
			else if (clazz.isInstance(ret))
				return clazz.cast(ret);
			else if (ret instanceof DelegatingStore)
				ret = ((DelegatingStore)ret).getActualStore(clazz);
			else
				return null;
		}
	}
	
	
	/**
	 * Returns the leaf {@link Store} of the delegation chain.
	 */
	public Store getDeepActualStore() {
		Store ret = this.getActualStore();
		while (ret instanceof DelegatingStore)
			ret = ((DelegatingStore)ret).getActualStore();
		return ret;
	}
	
	public void start() throws DatabaseNotReachedException {
		actualStore.start();
	}

	public boolean hasTable(String tableName)
			throws DatabaseNotReachedException {
		return actualStore.hasTable(tableName);
	}

	public void delete(MetaInformation meta, String table, String id)
			throws DatabaseNotReachedException {
		actualStore.delete(meta, table, id);
	}

	public boolean exists(MetaInformation meta, String table, String row)
			throws DatabaseNotReachedException {
		return actualStore.exists(meta, table, row);
	}

	public boolean exists(MetaInformation meta, String table, String row,
			String family) throws DatabaseNotReachedException {
		return actualStore.exists(meta, table, row, family);
	}

	public CloseableKeyIterator get(MetaInformation meta, String table,
			Constraint c, int limit, Set<String> families)
			throws DatabaseNotReachedException {
		return actualStore.get(meta, table, c, limit, families);
	}

	public byte[] get(MetaInformation meta, String table, String row,
			String family, String key) throws DatabaseNotReachedException {
		return actualStore.get(meta, table, row, family, key);
	}

	public Map<String, byte[]> get(MetaInformation meta, String table,
			String id, String family) throws DatabaseNotReachedException {
		return actualStore.get(meta, table, id, family);
	}

	public Map<String, byte[]> get(MetaInformation meta, String table,
			String id, String family, Constraint c)
			throws DatabaseNotReachedException {
		return actualStore.get(meta, table, id, family, c);
	}

	public ColumnFamilyData get(MetaInformation meta, String table, String id,
			Set<String> families) throws DatabaseNotReachedException {
		return actualStore.get(meta, table, id, families);
	}

	public void storeChanges(MetaInformation meta, String table, String id,
			ColumnFamilyData changed, Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> increments)
			throws DatabaseNotReachedException {
		actualStore.storeChanges(meta, table, id, changed, removed, increments);
	}

	public long count(MetaInformation meta, String table, Constraint c)
			throws DatabaseNotReachedException {
		return actualStore.count(meta, table, c);
	}
}
