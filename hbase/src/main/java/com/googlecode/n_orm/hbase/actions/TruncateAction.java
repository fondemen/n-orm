package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.hbase.async.DeleteRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.hbase.MangledTableName;
import com.googlecode.n_orm.hbase.Store;
import com.googlecode.n_orm.hbase.mapreduce.Truncator;
import com.stumbleupon.async.Deferred;

public class TruncateAction extends Action<Void> {
	private final Store store;
	private final Scan scan;
	private MangledTableName tableName;

	public TruncateAction(Store store, Scan scan) {
		super();
		this.store = store;
		this.scan = scan;
	}
	
	protected void truncateSimple() throws IOException  {
		ScanAction sc = new ScanAction(scan, tableName);
		Scan r =null;

		try {
			r = sc.getScan();
	
			final int nbRows = 100;
		    ArrayList<KeyValue>  res = r.next(nbRows);
			while (res != null && res.size() != 0) {
				for ( KeyValue result : res) {
					new DeleteRequest(tableName.getNameAsBytes(), result.key());
				}
				res = r.next(nbRows);
			}
		} finally {
			if (r != null)
				r.close();
		}
	}
	
	protected void truncateMapReduce() throws IOException, InterruptedException, ClassNotFoundException  {
		String table_Name = tableName.toString();
		Job count = Truncator.createSubmittableJob(this.store, table_Name, this.scan);
		if(!count.waitForCompletion(false)) {
			throw new DatabaseNotReachedException("Could not truncate table with map/reduce " + tableName);
		}
	}

	@Override
	public Deferred<Void> perform(HBaseClient client) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public MangledTableName getTable() {
		return tableName;
	}
	
	public void setTable(MangledTableName table){
		this.tableName=table;
	}

}
