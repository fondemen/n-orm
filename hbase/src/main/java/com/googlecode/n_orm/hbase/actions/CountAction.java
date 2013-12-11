package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;
import java.util.ArrayList;

import com.googlecode.n_orm.hbase.actions.Scan;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.hbase.MangledTableName;
import com.googlecode.n_orm.hbase.Store;
import com.googlecode.n_orm.hbase.mapreduce.RowCounter;
import com.stumbleupon.async.Deferred;

public class CountAction extends Action<Long> {
	public static int scanCaching = 1000;

	private final Store store;
	private final Scan scan;
	private MangledTableName tableName;

	public CountAction(Store store, Scan scan) {
		super();
		this.store = store;
		this.scan = scan;
	}

	protected long countSimple() throws IOException {
		this.scan.setCaching(scanCaching);
		ScanAction sc = new ScanAction(scan, tableName);
		Scan r = sc.getScan();
		int count = 0;
		try {
			while (true) {
				if (r.next() == null) // méthode next est à implementer
					return count;
				else
					count++;
			}
		} finally {
			if (r != null)
				r.close(); // méthodde close est à implementer
		}
	}

	protected  long countMapRed() throws DatabaseNotReachedException,
			IOException, InterruptedException, ClassNotFoundException {
		String table_Name = tableName.toString();
		Job count = RowCounter.createSubmittableJob(this.store, table_Name,
				this.scan);
		if (!count.waitForCompletion(false))
			throw new DatabaseNotReachedException("Row count failed for table "
					+ tableName);
		return count.getCounters()
				.findCounter(RowCounter.RowCounterMapper.Counters.ROWS)
				.getValue();
	}

	@Override
	public Deferred<Long> perform(HBaseClient client) throws Exception {
		
		if (this.store.isCountMapRed())
			return this.countMapRed();
		else
			return this.countSimple();

	}

	@Override
	public MangledTableName getTable() {
		return tableName;
	}
	
	public void setTable(MangledTableName table){
		this.tableName=table;
	}

}
