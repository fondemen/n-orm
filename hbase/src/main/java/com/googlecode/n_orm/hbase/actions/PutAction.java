package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;

import com.googlecode.n_orm.hbase.MangledTableName;
import com.stumbleupon.async.Deferred;

public class PutAction extends Action<Void> {
	
	private final PutRequest put;
	private MangledTableName tableName;
	private boolean b;

	public PutAction(PutRequest put) {
		super();
		this.put = put;
	}

	public PutRequest getPut() {
		return put;
	}
	
	@Override
	public Deferred<Void> perform(HBaseClient client) throws Exception {
		client.put(this.getPut());
		return null;	
	}
	
	@Override
	public MangledTableName getTable() {
		return tableName;
	}
	
	public void setTable(MangledTableName table){
		this.tableName=table;
	}

	public void setWriteToWAL(boolean c) {
		// TODO Auto-generated method stub
		
	}


	
}
