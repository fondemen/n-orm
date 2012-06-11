package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.Store;

public class DummyStore implements Store {
	
	public static final DummyStore INSTANCE = new DummyStore();
	
	private String id = "no id provided";
	private int id2 = 0;
	private boolean started = false;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getId2() {
		return id2;
	}

	public void setId2(int id2) {
		this.id2 = id2;
	}
	
	public boolean isStarted() {
		return this.started;
	}
	
	public void start() {
		this.started = true;
	}

	@Override
	public void delete(PersistingElement elt, String table, String id)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean exists(PersistingElement elt, String table, String row)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean exists(PersistingElement elt, Field columnFamily,
			String table, String row, String family)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte[] get(PersistingElement elt, Field property, String table,
			String row, String family, String key)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, byte[]> get(PersistingElement elt, Field columnFamily,
			String table, String id, String family)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, byte[]> get(PersistingElement elt, Field columnFamily,
			String table, String id, String family, Constraint c)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long count(Class<? extends PersistingElement> type, String table,
			Constraint c) throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public CloseableKeyIterator get(Class<? extends PersistingElement> type,
			String table, Constraint c, int limit, Map<String, Field> families)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnFamilyData get(PersistingElement elt,
			String table, String id, Map<String, Field> families)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void storeChanges(PersistingElement elt,
			Map<String, Field> changedFields, String table, String id,
			ColumnFamilyData changed,
			Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> increments)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		
	}

}
