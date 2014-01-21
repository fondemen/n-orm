package com.googlecode.n_orm.hbase.actions;

import org.hbase.async.HBaseClient;

import com.googlecode.n_orm.hbase.MangledTableName;
import com.stumbleupon.async.Deferred;

public abstract class Action<R> {
	/*
	 * A table to store data
	 * */
	private MangledTableName tableName;
	/**
	 * perform the action in this table
	 * @param client
	 * @return
	 * @throws Exception
	 */
	public abstract Deferred<R> perform(HBaseClient client) throws Exception;
	/**
	 * 
	 * @return the table
	 */
	public abstract MangledTableName getTable();
}