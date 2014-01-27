package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.hbase.async.DeleteRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

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

	//
	protected void truncateSimple() throws IOException {
		Scanner r = null;
		
		try {
			// initialisation du Scanner
			if(this.scan.getFamily()!=null){
				r.setFamily(this.scan.getFamily());
			}
			if(this.scan.getStartRow()!=null){
				r.setStartKey(this.scan.getStartRow());
			}
			if(this.scan.getStopRow()!=null){
				r.setStopKey(this.scan.getStopRow());
			}
			if(this.scan.getQualifier()!=null){
				r.setQualifier(this.scan.getQualifier());
			}
			if(this.scan.getQualifiers()!=null){
				r.setQualifiers(this.scan.getQualifiers());
			}
			
			final int nbRows = 100;
			DeleteAction da;
			List<DeleteRequest> dels = new ArrayList<DeleteRequest>(nbRows);
			Deferred<ArrayList<ArrayList<KeyValue>>> res = r.nextRows(nbRows);
			ArrayList<ArrayList<KeyValue>> resultRow = res.join();
			while (resultRow != null && resultRow.size() != 0) {
				dels.clear();
				for (ArrayList<KeyValue> r1 : resultRow) {
					for (KeyValue kv : r1) {
						dels.add(new DeleteRequest(tableName.getNameAsBytes(),
								kv.key()));

					}
				}
				for (DeleteRequest del : dels) {
					da = new DeleteAction(del);
				}

			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (r != null)
				r.close();
		}
	}

	protected void truncateMapReduce() throws IOException,
			InterruptedException, ClassNotFoundException {
		String table_Name = tableName.toString();
		Job count = Truncator.createSubmittableJob(this.store, table_Name,
				this.scan);
		if (!count.waitForCompletion(false)) {
			throw new DatabaseNotReachedException(
					"Could not truncate table with map/reduce " + tableName);
		}
	}

	@Override
	public Deferred<Void> perform(HBaseClient client) throws Exception {
		if (this.store.isTruncateMapRed())
			this.truncateMapReduce();
		else
			this.truncateSimple();
		return null;
	}

	@Override
	public MangledTableName getTable() {
		return tableName;
	}

	public void setTable(MangledTableName table) {
		this.tableName = table;
	}

}
