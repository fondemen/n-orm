package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Row;

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
		for (Entry<byte[], NavigableMap<byte[], Long>> fam : this.getIncrements().getFamilyMap().entrySet()) {
			for (Entry<byte[], Long> inc : fam.getValue().entrySet()) {
				this.getTable().incrementColumnValue(this.getIncrements().getRow(), fam.getKey(), inc.getKey(), inc.getValue());
			}
		}
		
		return null;
	}
	
}