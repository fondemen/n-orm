package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.MetaInformation;
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
	public void delete(MetaInformation meta, String table, String id)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean exists(MetaInformation meta, String table, String row)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean exists(MetaInformation meta, String table, String row,
			String family) throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte[] get(MetaInformation meta, String table, String row,
			String family, String key) throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, byte[]> get(MetaInformation meta, String table,
			String id, String family) throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, byte[]> get(MetaInformation meta, String table,
			String id, String family, Constraint c)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long count(MetaInformation meta, String table,
			Constraint c) throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public CloseableKeyIterator get(MetaInformation meta, String table,
			Constraint c, int limit, Set<String> families)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnFamilyData get(MetaInformation meta, String table, String id,
			Set<String> families) throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void storeChanges(MetaInformation meta, String table, String id,
			ColumnFamilyData changed, Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> increments)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasTable(String tableName)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return false;
	}

}
