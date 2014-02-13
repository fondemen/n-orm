package com.googlecode.n_orm.cache.read;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
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
	
	/**
	 * Whether read cache should be enabled for a given thread
	 */
	private static ThreadLocal<Boolean> enabled = new ThreadLocal<Boolean>();

	/**
	 * Whether read cache should be enabled for this thread even for
	 * {@link #setEnabledByDefault(boolean) de-activated} read cache stores.
	 */
	public static boolean isEnabledForCurrentThread() {
		Boolean ret = enabled.get();
		return ret != null && ret;
	}

	/**
	 * Whether cache should be enabled for this thread even for
	 * {@link #setEnabledByDefault(boolean) de-activated} write-retention stores.
	 */
	public static void setEnabledForCurrentThread(boolean enabled) {
		CachedStore.enabled.set(enabled);
	}
	

	/**
	 * The actual cache used
	 */
	private ICache cache;
	
	/**
	 * Whether read cache should be enabled by default
	 */
	private boolean enabledByDefault = true;

	/**
	 * {{@link #createCache()} must be overridden to use this constructor.
	 * @param actualStore
	 */
	public CachedStore(Store actualStore) {
		this(actualStore, null);
	}

	public CachedStore(Store actualStore, ICache cache) {
		super(actualStore);
		this.cache = cache;
	}

	/**
	 * The actual cache used
	 */
	public ICache getCache() {
		return cache;
	}

	/**
	 * Called at store {{@link #start() startup} when constructor {{@link #CachedStore(Store)} was invoked.
	 * Does not need to be thread safe.
	 * @return the cache to be used.
	 */
	protected ICache createCache() {
		try {
			throw new UnsupportedOperationException(this.getClass().getName() +" does not implements " + CachedStore.class.getMethod("createCache"));
		} catch (SecurityException e) {
			throw new RuntimeException(e);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void start() throws DatabaseNotReachedException {
		if (this.cache == null) {
			synchronized(this) {
				if (this.cache == null) {
					this.cache = this.createCache();
				}
			}
		}
		super.start();
	}

	/**
	 * Whether read cache is enabled by default for thread that did not call {@link #setEnabledForCurrentThread(boolean)}.
	 */
	public boolean isEnabledByDefault() {
		return this.enabledByDefault;
	}


	/**
	 * Whether read cache is enabled by default for thread that did not call {@link #setEnabledForCurrentThread(boolean)}.
	 */
	public void setEnabledByDefault(boolean enabled) {
		this.enabledByDefault = enabled;
	}

	/**
	 * Whether this store is actually caching.
	 * It will return true if store is
	 * {@link #setEnabledByDefault(boolean) enabled} or thread is
	 * {@link #setEnabledForCurrentThread(boolean) enabled}.
	 */
	private boolean isCaching() {
		return this.cache != null && this.isEnabledByDefault() || isEnabledForCurrentThread();
	}

	public void delete(MetaInformation meta, String table, String id)
			throws DatabaseNotReachedException {
		DatabaseNotReachedException tbt = null;
		if (isCaching()) {
			try {
				cache.delete(meta, table, id);
			} catch (CacheException e) {
				tbt = new DatabaseNotReachedException(e);
			}
		}
		
		super.delete(meta, table, id);
		
		if (tbt != null) {
			throw tbt;
		}
	}

	/**
	 * check if an element exist in the cache
	 */
	public boolean exists(MetaInformation meta, String table, String row,
			String family) throws DatabaseNotReachedException {
		if (!isCaching()) {
			return super.exists(meta, table, row, family);
		}
		
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
		if (!isCaching()) {
			return super.get(meta, table, id, family);
		}
		
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
			throw new DatabaseNotReachedException(e);
		}
	}

	public boolean exists(MetaInformation meta, String table, String row)
			throws DatabaseNotReachedException {
		if (!isCaching()) {
			return super.exists(meta, table, row);
		}
		
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

	public ColumnFamilyData get(MetaInformation meta, String table,
			String id, Set<String> families)
			throws DatabaseNotReachedException {
		if (!isCaching()) {
			return super.get(meta, table, id, families);
		}
		
		try {
			DefaultColumnFamilyData ret = new DefaultColumnFamilyData();
			Set<String> familiesName = new TreeSet<String>(families);
			Iterator<String> it = familiesName.iterator();

			while (it.hasNext()) {
				String name = it.next();
				Map<String, byte[]> data = cache.getFamilyData(meta, table, id, name);
				if (data != null) {
					ret.put(name, data);
					it.remove();
				}
			}

			if (!familiesName.isEmpty()) {
				ColumnFamilyData dataStore = super.get(meta, table, id,
						familiesName);

				for (Entry<String, Map<String, byte[]>> cfd : dataStore
						.entrySet()) {
					ret.put(cfd.getKey(), cfd.getValue());
					cache.insertFamilyData(meta, table, id, cfd.getKey(),
							cfd.getValue());
				}
			}

			return ret;
		} catch (CacheException e) {
			throw new DatabaseNotReachedException(e);
		}
	}
}
