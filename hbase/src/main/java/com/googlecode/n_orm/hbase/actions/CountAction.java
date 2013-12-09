package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

import org.apache.hadoop.hbase.client.ResultScanner;
import com.googlecode.n_orm.hbase.actions.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.hbase.async.HBaseClient;
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
	/***
	 * 
	 * @return the number of rows which are scan
	 * @throws IOException
	 */
	protected long countSimple( MangledTableName tableName ) throws IOException {
		this.scan.setCaching(scanCaching);
		Scanner scr=new Scanner();
		
		ResultScanner r = getTable().getScanner(this.scan);
		int count = 0;
		try {
			while (true) {
				if (r.next() == null)
					return count;
				else
					count++;
			}
		} finally {
			if (r != null)
				r.close();
		}
	}
	
	protected long countMapRed() throws DatabaseNotReachedException, IOException, InterruptedException, ClassNotFoundException {
		String tableName = Bytes.toString(getTable().getTableName());
		Job count = RowCounter.createSubmittableJob(this.store, tableName, this.scan);
		if(!count.waitForCompletion(false))
			throw new DatabaseNotReachedException("Row count failed for table " + tableName);
		return count.getCounters().findCounter(RowCounter.RowCounterMapper.Counters.ROWS).getValue();
	}

	@Override
	public Deferred<Long> perform(HBaseClient client) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
