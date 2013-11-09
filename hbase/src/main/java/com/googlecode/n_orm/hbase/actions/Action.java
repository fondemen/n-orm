package com.googlecode.n_orm.hbase.actions;

import org.hbase.async.HBaseClient;

import com.stumbleupon.async.Deferred;

public abstract class Action<R> {
	private HBaseClient table;
	
	public HBaseClient getTable() {
		return this.table;
	}
	
	public void setTable(HBaseClient table) {
		this.table = table;
	}
	
	public abstract Deferred<R> perform() throws Exception;
}