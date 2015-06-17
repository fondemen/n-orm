package com.googlecode.n_orm.hbase.actions;

import org.apache.hadoop.hbase.client.Table;

public abstract class Action<R> {
	private Table table;
	
	public Table getTable() {
		return this.table;
	}
	
	public void setTable(Table table) {
		this.table = table;
	}
	
	public abstract R perform() throws Exception;
}