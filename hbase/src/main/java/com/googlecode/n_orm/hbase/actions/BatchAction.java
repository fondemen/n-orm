
package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;

import com.googlecode.n_orm.hbase.MangledTableName;
import com.stumbleupon.async.Deferred;

public class BatchAction extends Action<Void> {
	/***
	 * a list of actions: an action can be a delete, put, get, increments....
	 */
	private final List<Action> actions;
	
	private MangledTableName tableName;

	public BatchAction(List<Action> batch) {
		super();
		this.actions=batch;
		
	}

	public List<Action> getBatch() {
		return actions;
	}
	
	@Override
	public Deferred<Void> perform(HBaseClient client) throws Exception {
		for(Action a: actions){
			a.perform(client);
		}
		
		return null;
	}

	@Override
	public MangledTableName getTable() {
		return tableName;
	}
	
	public void setTable(MangledTableName table){
		this.tableName=table;
	}

	
}