package com.googlecode.n_orm.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;

import com.googlecode.n_orm.hbase.Store;

public class Truncator {

	static final String NAME = "truncator";
	public static class TruncatorMapper extends
			TableMapper<ImmutableBytesWritable, Delete> {
		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {
			for (@SuppressWarnings("unused")
			KeyValue value : values.list()) {
				Delete d = new Delete(row.get());
				try {
					context.write(row, d);
					break;
				} catch (InterruptedException e) {
				}
			}
		}
	}
	
	public static final class DeleteWriter extends LocalTableRecordWriter<Delete> {

		public DeleteWriter(HTable table) {
			super(table);
		}

		@Override
		public void write(ImmutableBytesWritable row, Delete delete)
				throws IOException, InterruptedException {
			this.getTable().delete(delete);
		}
		
	}

	public static Job createSubmittableJob(Store s,
			String tableName, Scan scan) throws IOException {
		Configuration conf = LocalFormat.prepareConf(s, DeleteWriter.class);
		Job job = new Job(conf, NAME + "_" + tableName + "_" + scan.hashCode());
		scan.setCaching(500);
		TableMapReduceUtil.initTableMapperJob(tableName, scan,
				TruncatorMapper.class, ImmutableBytesWritable.class,
				Delete.class, job, true);
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(Delete.class);
	    LocalFormat.prepareJob(job);
		job.setNumReduceTasks(0);
		return job;
	}
}
