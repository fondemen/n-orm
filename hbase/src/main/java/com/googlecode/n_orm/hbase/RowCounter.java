package com.googlecode.n_orm.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class RowCounter {

	static final String NAME = "rowcounter";

	public static class RowCounterMapper extends
			TableMapper<ImmutableBytesWritable, Result> {

		public static enum Counters {
			ROWS
		}

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {
			for (@SuppressWarnings("unused") KeyValue value : values.list()) {
				context.getCounter(Counters.ROWS).increment(1);
				break;
			}
		}
	}

	public static Job createSubmittableJob(Configuration conf, String tableName,
			Scan scan) throws IOException {
		Job job = new Job(conf, NAME + "_" + tableName + "_" + scan.hashCode());
		scan.setCaching(500);
		job.setJarByClass(RowCounter.class);
		scan.setFilter(new FirstKeyOnlyFilter());
		job.setOutputFormatClass(NullOutputFormat.class);
		TableMapReduceUtil.initTableMapperJob(tableName, scan,
				RowCounterMapper.class, ImmutableBytesWritable.class,
				Result.class, job);
		job.setNumReduceTasks(0);
		return job;
	}
}
