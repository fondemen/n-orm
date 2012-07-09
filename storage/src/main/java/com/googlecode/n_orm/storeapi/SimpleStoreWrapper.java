package com.googlecode.n_orm.storeapi;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;

public class SimpleStoreWrapper implements Store {
	private static Map<SimpleStore, Store> INSTANCES = new HashMap<SimpleStore, Store>();

	public static SimpleStoreWrapper getWrapper(SimpleStore s) {
		synchronized (INSTANCES) {
			SimpleStoreWrapper ret = (SimpleStoreWrapper) INSTANCES
					.get(s);
			if (ret == null) {
				ret = new SimpleStoreWrapper(s);
			}
			return ret;
		}
	}

	private final SimpleStore store;

	protected SimpleStoreWrapper() {
		if (!(this instanceof SimpleStore))
			throw new IllegalStateException(this.getClass().getName()
					+ " must implement " + SimpleStore.class.getName());

		synchronized (INSTANCES) {
			INSTANCES.put((SimpleStore) this, this);
			this.store = (SimpleStore) this;
		}
	}

	private SimpleStoreWrapper(SimpleStore store) {
		synchronized (INSTANCES) {
			INSTANCES.put(store, this);
			this.store = store;
		}
	}

	public SimpleStore getStore() {
		return this.store;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.googlecode.n_orm.storeapi.TypeAwareStore#start()
	 */
	@Override
	public void start() throws DatabaseNotReachedException {
		store.start();
	}
	
	@Override
	public boolean hasTable(String tableName) throws DatabaseNotReachedException {
		return this.store.hasTable(tableName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.googlecode.n_orm.storeapi.TypeAwareStore#delete(com.googlecode.n_orm
	 * .PersistingElement, java.lang.String, java.lang.String)
	 */
	@Override
	public void delete(PersistingElement elt, String table, String id)
			throws DatabaseNotReachedException {
		this.store.delete(table, id);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.googlecode.n_orm.storeapi.TypeAwareStore#exists(com.googlecode.n_orm
	 * .PersistingElement, java.lang.String, java.lang.String)
	 */
	@Override
	public boolean exists(PersistingElement elt, String table, String row)
			throws DatabaseNotReachedException {
		return this.store.exists(table, row);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.googlecode.n_orm.storeapi.TypeAwareStore#exists(com.googlecode.n_orm
	 * .PersistingElement, java.lang.reflect.Field, java.lang.String,
	 * java.lang.String, java.lang.String)
	 */
	@Override
	public boolean exists(PersistingElement elt, Field columnFamily,
			String table, String row, String family)
			throws DatabaseNotReachedException {
		return this.store.exists(table, row, family);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.googlecode.n_orm.storeapi.TypeAwareStore#get(java.lang.Class,
	 * java.util.Set, java.lang.String,
	 * com.googlecode.n_orm.storeapi.Constraint, int, java.util.Set)
	 */
	@Override
	public CloseableKeyIterator get(Class<? extends PersistingElement> type,
			String table, Constraint c, int limit,
			Map<String, Field> families) throws DatabaseNotReachedException {
		return this.store.get(table, c, limit, families == null ? null : families.keySet());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.googlecode.n_orm.storeapi.TypeAwareStore#get(com.googlecode.n_orm
	 * .PersistingElement, java.lang.reflect.Field, java.lang.String,
	 * java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public byte[] get(PersistingElement elt, Field property, String table,
			String row, String family, String key)
			throws DatabaseNotReachedException {
		return this.store.get(table, row, family, key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.googlecode.n_orm.storeapi.TypeAwareStore#get(com.googlecode.n_orm
	 * .PersistingElement, java.lang.reflect.Field, java.lang.String,
	 * java.lang.String, java.lang.String)
	 */
	@Override
	public Map<String, byte[]> get(PersistingElement elt, Field columnFamily,
			String table, String id, String family)
			throws DatabaseNotReachedException {
		return store.get(table, id, family);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.googlecode.n_orm.storeapi.TypeAwareStore#get(com.googlecode.n_orm
	 * .PersistingElement, java.lang.reflect.Field, java.lang.String,
	 * java.lang.String, java.lang.String,
	 * com.googlecode.n_orm.storeapi.Constraint)
	 */
	@Override
	public Map<String, byte[]> get(PersistingElement elt, Field columnFamily,
			String table, String id, String family, Constraint c)
			throws DatabaseNotReachedException {
		return store.get(table, id, family, c);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.googlecode.n_orm.storeapi.TypeAwareStore#get(com.googlecode.n_orm
	 * .PersistingElement, java.lang.String, java.lang.String, java.util.Set)
	 */
	@Override
	public ColumnFamilyData get(PersistingElement elt,
			String table, String id,
			Map<String, Field> families) throws DatabaseNotReachedException {
		return store.get(table, id, families == null ? null : families.keySet());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.googlecode.n_orm.storeapi.TypeAwareStore#storeChanges(com.googlecode
	 * .n_orm.PersistingElement, java.lang.String, java.lang.String,
	 * java.util.Map, java.util.Map, java.util.Map)
	 */
	@Override
	public void storeChanges(PersistingElement elt, Map<String, Field> changedFields,
			String table, String id, ColumnFamilyData changed,
			Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> increments)
			throws DatabaseNotReachedException {
		store.storeChanges(table, id, changed, removed, increments);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.googlecode.n_orm.storeapi.TypeAwareStore#count(java.lang.Class,
	 * java.lang.String, com.googlecode.n_orm.storeapi.Constraint)
	 */
	@Override
	public long count(Class<? extends PersistingElement> type, String table,
			Constraint c) throws DatabaseNotReachedException {
		return store.count(table, c);
	}
}
