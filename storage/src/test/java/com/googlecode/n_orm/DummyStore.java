package com.googlecode.n_orm;

import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Store;
import com.googlecode.n_orm.storeapi.Constraint;

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
	public byte[] get(String table, String row, String family, String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void delete(String table, String id)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean exists(String table, String row, String family)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean exists(String table, String row)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public CloseableKeyIterator get(String table, Constraint c, int limit, Set<String> families)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void storeChanges(String table, String id,
			Map<String, Map<String, byte[]>> changed,
			Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> increments)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Map<String, byte[]>> get(String table, String id,
			Set<String> families) throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, byte[]> get(String table, String id, String family)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, byte[]> get(String table, String id, String family,
			Constraint c) throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return null;
	}

}
