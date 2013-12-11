package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;



//import org.apache.hadoop.hbase.client.Increment;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.HBaseClient;

import com.googlecode.n_orm.hbase.MangledTableName;
import com.stumbleupon.async.Deferred;

public class IncrementAction extends Action<Void> {
	
	private final AtomicIncrementRequest incr;
	private MangledTableName tableName;

	public IncrementAction(AtomicIncrementRequest incr) {
		super();
		this.incr = incr;
	}

	public AtomicIncrementRequest getIncrements() {
		return incr;
	}

	@Override
	public Deferred<Void> perform(HBaseClient client) throws Exception {
		client.atomicIncrement(this.incr);
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