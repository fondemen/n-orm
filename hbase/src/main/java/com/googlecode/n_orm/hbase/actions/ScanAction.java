package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


public class ScanAction extends Action<ResultScanner> {
	
	private final Scan scan;

	public ScanAction(Scan scan) {
		super();
		this.scan = scan;
	}

	public Scan getScan() {
		return scan;
	}

	@Override
	public ResultScanner perform() throws IOException {
		return this.getTable().getScanner(this.getScan());
	}
	
}