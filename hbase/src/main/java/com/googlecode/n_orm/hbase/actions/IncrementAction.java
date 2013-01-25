package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Increment;

public class IncrementAction extends Action<Void> {
	
	private final Increment incr;

	public IncrementAction(Increment incr) {
		super();
		this.incr = incr;
	}

	public Increment getIncrements() {
		return incr;
	}

	@Override
	public Void perform() throws IOException, InterruptedException {
		this.getTable().increment(this.getIncrements());
		return null;
	}
	
}