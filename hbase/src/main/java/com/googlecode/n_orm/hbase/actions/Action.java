package com.googlecode.n_orm.hbase.actions;

import org.apache.hadoop.hbase.client.HTable;

public abstract class Action<R> {
	private HTable table;
	
	public HTable getTable() {
		return this.table;
	}
	
	public void setTable(HTable table) {
		this.table = table;
	}
	
	public abstract R perform() throws Exception;
}