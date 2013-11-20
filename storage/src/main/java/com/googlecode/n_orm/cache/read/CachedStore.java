package com.googlecode.n_orm.cache.read;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.DelegatingStore;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.Store;

public class CachedStore extends DelegatingStore {
	private final ICache cache;

	public CachedStore(Store actualStore, ICache cache) {
		super(actualStore);
		this.cache = cache;
	}

	public ICache getCache() {
		return cache;
	}

	public void delete(MetaInformation meta, String table, String id)
			throws DatabaseNotReachedException {
		try {
			cache.delete(meta, table, id);
			super.delete(meta, table, id);
		} catch (CacheException e) {
			throw new DatabaseNotReachedException("Error while removing "
					+ meta.getElement() + " from cache: " + e.getMessage(), e);
		}

	}

	/**
	 * check if an element exist in the cache
	 */
	public boolean exists(MetaInformation meta, String table, String row,
			String family) throws DatabaseNotReachedException {
		try {
			if (cache.existsData(meta, table, row, family))
				return true;
		} catch (CacheException e) {
			throw new DatabaseNotReachedException(e);
		}
		return super.exists(meta, table, row, family);
	}

	/*
	 * Check if an element is in the cache or in the store.
	 * 
	 * @see
	 * com.googlecode.n_orm.storeapi.DelegatingStore#get(com.googlecode.n_orm
	 * .storeapi.MetaInformation, java.lang.String, java.lang.String,
	 * java.lang.String)
	 */
	public Map<String, byte[]> get(MetaInformation meta, String table,
			String id, String family) throws DatabaseNotReachedException {
		try {
			Map<String, byte[]> data = cache.getFamilyData(meta, table, id,
					family);
			if (data != null) {
				return data;
			} else {
				data = getActualStore().get(meta, table, id, family);
				cache.insertFamilyData(meta, table, id, family, data);
				assert data.equals(cache.getFamilyData(meta, table, id, family));
				return data;
			}
		} catch (CacheException e) {
			throw new DatabaseNotReachedException("No family Data");
		}
	}

	public boolean exists(MetaInformation meta, String table, String row)
			throws DatabaseNotReachedException {
		Collection<ColumnFamily<?>> cfs = meta.getElement().getColumnFamilies();
		for (ColumnFamily<?> columnFamily : cfs) {
			String name = columnFamily.getName();
			try {
				if (cache.existsData(meta, table, row, name)) {
					return true;
				}
			} catch (CacheException e) {
				throw new DatabaseNotReachedException(e);
			}
		}
		return super.exists(meta, table, row);
	}

	public ColumnFamilyData get(MetaInformation meta, String table, String id,
			Set<String> families) throws DatabaseNotReachedException {
		try {
			DefaultColumnFamilyData dcfd = new DefaultColumnFamilyData();
			Set<String> familiesName = new TreeSet<String>(families);
			Iterator<String> it = familiesName.iterator();
			Map<String, byte[]> data = new HashMap<String, byte[]>();

			while (it.hasNext()) {
				String name = it.next();
				data = cache.getFamilyData(meta, table, id, name);
				if (data != null) {
					dcfd.put(name, data);
					it.remove();
				}
			}
			ColumnFamilyData dataStore = super.get(meta, table, id,
					familiesName);

			for (Entry<String, Map<String, byte[]>> cfd : dataStore.entrySet()) {
				dcfd.put(cfd.getKey(), cfd.getValue());
				cache.insertFamilyData(meta, table, id, cfd.getKey(),
						cfd.getValue());
			}
			return dcfd;
		} catch (CacheException e) {
			throw new DatabaseNotReachedException(e);
		}
	}
}
