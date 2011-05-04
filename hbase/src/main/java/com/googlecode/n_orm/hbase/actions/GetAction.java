package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

public class GetAction extends Action<Result> {
	
	private final Get get;

	public GetAction(Get get) {
		super();
		this.get = get;
	}

	public Get getGet() {
		return get;
	}

	@Override
	public Result perform() throws IOException {
		return this.getTable().get(this.getGet());
	}
	
}