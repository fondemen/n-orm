package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;

public class ExistsAction extends Action<Boolean> {
	
	private final Get get;

	public ExistsAction(Get get) {
		super();
		this.get = get;
	}

	public Get getGet() {
		return get;
	}

	@Override
	public Boolean perform() throws IOException {
		return this.getTable().exists(this.getGet());
	}
	
}