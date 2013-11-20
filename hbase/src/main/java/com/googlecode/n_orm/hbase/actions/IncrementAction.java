package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

//import org.apache.hadoop.hbase.client.Increment;
import org.hbase.async.AtomicIncrementRequest;

import com.stumbleupon.async.Deferred;

public class IncrementAction extends Action<Void> {
	
	private final AtomicIncrementRequest incr;

	public IncrementAction(AtomicIncrementRequest incr) {
		super();
		this.incr = incr;
	}

	public AtomicIncrementRequest getIncrements() {
		return incr;
	}

	@Override
	public Deferred<Void> perform() throws IOException, InterruptedException {
		this.getClient().atomicIncrement(this.getIncrements());
		return null;
	}
	
}