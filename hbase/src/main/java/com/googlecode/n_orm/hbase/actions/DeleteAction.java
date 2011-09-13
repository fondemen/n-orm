package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;

public class DeleteAction extends Action<Void> {
	
	private final Delete delete;

	public DeleteAction(Delete delete) {
		super();
		this.delete = delete;
	}

	public Delete getDelete() {
		return delete;
	}

	@Override
	public Void perform() throws IOException {
		this.getTable().delete(this.getDelete());
		return null;
	}
	
}