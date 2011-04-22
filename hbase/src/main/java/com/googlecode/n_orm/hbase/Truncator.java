package com.googlecode.n_orm.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class Truncator {

	static final String NAME = "truncator";

	public static class TruncatorMapper extends
			TableMapper<ImmutableBytesWritable, Delete> {
		
		private HTable table;
		
		
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			this.table = new HTable(new HBaseConfiguration(context.getConfiguration()), context.getConfiguration().get("table-deleted"));
		}



		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {
			for (@SuppressWarnings("unused") KeyValue value : values.list()) {
		      Delete d = new Delete(row.get());
		      this.table.delete(d);
		      break;
			}
		}
	}

	public static Job createSubmittableJob(Configuration conf, String tableName, Scan scan) throws IOException {
		conf = new Configuration(conf);
		conf.set("table-deleted", tableName);
		Job job = new Job(conf, NAME + "_" + tableName + "_" + scan.hashCode());
		job.setJarByClass(Truncator.class);
		scan.setCaching(500);
		job.setOutputFormatClass(NullOutputFormat.class);
		TableMapReduceUtil.initTableMapperJob(tableName, scan,
				TruncatorMapper.class, ImmutableBytesWritable.class,
				Delete.class, job);
//	    TableMapReduceUtil.initTableReducerJob(
//	           tableName, null, job,
//	            null, null, null, null);
		job.setNumReduceTasks(0);
		return job;
	}
}
