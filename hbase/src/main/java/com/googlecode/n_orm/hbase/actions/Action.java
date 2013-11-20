package com.googlecode.n_orm.hbase.actions;

import org.hbase.async.HBaseClient;

import com.stumbleupon.async.Deferred;

public abstract class Action<R> {
	public abstract Deferred<R> perform(HBaseClient client) throws Exception;
}