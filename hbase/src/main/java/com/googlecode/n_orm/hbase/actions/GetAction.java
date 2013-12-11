package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;
import java.util.ArrayList;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.GetRequest;

import com.googlecode.n_orm.hbase.MangledTableName;
import com.stumbleupon.async.Deferred;

public class GetAction extends Action<ArrayList<KeyValue>> {
	
	private final GetRequest get;
	private MangledTableName tableName; 

	public GetAction(GetRequest get) {
		super();
		this.get = get;
	}

	public GetRequest getGet() {
		return get;
	}

	@Override
	public Deferred<ArrayList<KeyValue>> perform(HBaseClient client) throws IOException {
		return client.get(this.getGet());
	}

	@Override
	public MangledTableName getTable() {
		return tableName;
	}
	
	public void setTable(MangledTableName table){
		this.tableName=table;
	}

	
}