package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

import org.hbase.async.DeleteRequest;
import org.hbase.async.HBaseClient;

import com.googlecode.n_orm.hbase.MangledTableName;
import com.stumbleupon.async.Deferred;

public class DeleteAction extends Action<Object> {
	
	private final DeleteRequest delete;
	private MangledTableName tableName;

	public DeleteAction(DeleteRequest delete) {
		super();
		this.delete = delete;
	}

	public DeleteRequest getDelete() {
		return delete;
	}

	/**
	 * perform a delete action 
	 */

	@Override
	public Deferred<Object> perform(HBaseClient client) throws Exception {
		Deferred<Object> Object = client.delete(this.getDelete());
		return Object;
	}
	
	@Override
	public MangledTableName getTable() {
		return tableName;
	}
	
	public void setTable(MangledTableName table){
		this.tableName=table;
	}
	
}