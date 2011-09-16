package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Row;

public class BatchAction extends Action<Void> {
	
	private final List<org.apache.hadoop.hbase.client.Row> batch;

	public BatchAction(List<org.apache.hadoop.hbase.client.Row> batch) {
		super();
		this.batch = batch;
	}

	public List<org.apache.hadoop.hbase.client.Row> getBatch() {
		return batch;
	}

	@Override
	public Void perform() throws IOException, InterruptedException {
		this.getTable().batch(this.getBatch());
		return null;
	}
	
}