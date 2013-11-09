package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.hbase.async.DeleteRequest;

import com.stumbleupon.async.Deferred;

public class DeleteAction extends Action<Void> {
	
	private final DeleteRequest delete;

	public DeleteAction(DeleteRequest delete) {
		super();
		this.delete = delete;
	}

	public DeleteRequest getDelete() {
		return delete;
	}

	@Override
	public Void perform() throws IOException {
		this.getTable().delete(this.getDelete());
		return null;
	}
	
}