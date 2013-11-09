package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.hbase.async.PutRequest;

import com.stumbleupon.async.Deferred;

public class PutAction extends Action<Void> {
	
	private final PutRequest put;

	public PutAction(PutRequest put) {
		super();
		this.put = put;
	}

	public PutRequest getPut() {
		return put;
	}

	@Override
	public Deferred<Void> perform() throws IOException {
		this.getTable().put(this.getPut());
		return null;
	}
	
}
