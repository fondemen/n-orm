package com.googlecode.n_orm.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.hbase.async.DeleteRequest;

import com.googlecode.n_orm.hbase.MangledTableName;
import com.googlecode.n_orm.hbase.Store;
import com.googlecode.n_orm.hbase.actions.Scan;

public class Truncator {

	static final String NAME = "truncator";
	public static class TruncatorMapper extends
			TableMapper<ImmutableBytesWritable, Void> {
		private MangledTableName table;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			LocalFormat lf = new LocalFormat();
			lf.setConf(context.getConfiguration());
			this.table = lf.getTable();
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			this.table.flushCommits();
			super.cleanup(context);
		}
		
		public void map(ImmutableBytesWritable row, ArrayList<KeyValue> values, Context context) throws IOException {
			for (@SuppressWarnings("unused")
			KeyValue value : values) {
				new DeleteRequest(value.getKey(),table.getNameAsBytes());
			}
		}
	}

	public static Job createSubmittableJob(Store s,
			String tableName, Scan scan) throws IOException {
		Configuration conf = LocalFormat.prepareConf(s, null);
		Job job = new Job(conf, NAME + "_" + tableName + "_" + scan.hashCode());
		/*TableMapReduceUtil.initTableMapperJob(tableName, scan,
				TruncatorMapper.class, ImmutableBytesWritable.class,
				DeleteRequest.class, job, false);
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(DeleteRequest.class);
	    LocalFormat.prepareJob(job, scan, s);
		job.setNumReduceTasks(0);*/
		return job;
	}
}
