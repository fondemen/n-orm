package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

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

	/**
	 * perform a delete action 
	 */
	@Override
	public Deferred<Void> perform() throws IOException {
		this.getClient().delete(this.getDelete());
		return null;
	}
	
}