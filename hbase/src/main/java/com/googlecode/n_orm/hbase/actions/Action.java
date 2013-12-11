package com.googlecode.n_orm.hbase.actions;

import org.hbase.async.HBaseClient;

import com.googlecode.n_orm.hbase.MangledTableName;
import com.stumbleupon.async.Deferred;

public abstract class Action<R> {
	private MangledTableName tableName;
	public abstract Deferred<R> perform(HBaseClient client) throws Exception;
	public abstract MangledTableName getTable();
}