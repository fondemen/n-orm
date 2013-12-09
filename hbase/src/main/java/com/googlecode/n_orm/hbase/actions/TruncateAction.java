package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.hbase.async.HBaseClient;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.hbase.Store;
import com.googlecode.n_orm.hbase.mapreduce.Truncator;
import com.stumbleupon.async.Deferred;

public class TruncateAction extends Action<Void> {
	private final Store store;
	private final Scan scan;

	public TruncateAction(Store store, Scan scan) {
		super();
		this.store = store;
		this.scan = scan;
	}
	
	protected void truncateSimple() throws IOException  {
		ResultScanner r = null;

		try {
			r = this.getTable().getScanner(this.scan);
			final int nbRows = 100;
			List<Delete> dels = new ArrayList<Delete>(nbRows);
			Result [] res = r.next(nbRows);
			while (res != null && res.length != 0) {
				dels.clear();
				for (Result result : res) {
					dels.add(new Delete(result.getRow()));
				}
				getTable().delete(dels);
				res = r.next(nbRows);
			}
		} finally {
			if (r != null)
				r.close();
		}
	}
	
	protected void truncateMapReduce() throws IOException, InterruptedException, ClassNotFoundException  {
		String tableName = Bytes.toString(getTable().getTableName());
		Job count = Truncator.createSubmittableJob(this.store, tableName, this.scan);
		if(!count.waitForCompletion(false)) {
			throw new DatabaseNotReachedException("Could not truncate table with map/reduce " + tableName);
		}
	}

	@Override
	public Deferred<Void> perform(HBaseClient client) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
