package com.googlecode.n_orm.hbase.actions;

import org.apache.hadoop.hbase.client.HTableInterface;

public abstract class Action<R> {
	private HTableInterface table;
	
	public HTableInterface getTable() {
		return this.table;
	}
	
	public void setTable(HTableInterface table) {
		this.table = table;
	}
	
	public abstract R perform() throws Exception;
}