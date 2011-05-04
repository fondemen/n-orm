package com.googlecode.n_orm.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

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

	public static class TruncatorOutputFormat extends
			OutputFormat<ImmutableBytesWritable, Delete> implements
			Configurable {

		protected static class TableRecordWriter extends
				RecordWriter<ImmutableBytesWritable, Delete> {

			private HTable table;

			public TableRecordWriter(HTable table) {
				this.table = table;
			}

			@Override
			public void close(TaskAttemptContext ctx) throws IOException,
					InterruptedException {
				table.flushCommits();
			}

			@Override
			public void write(ImmutableBytesWritable row, Delete value)
					throws IOException, InterruptedException {
				this.table.delete(new Delete(value));
			}

		}

		private HTable table;
		private Configuration conf;

		@Override
		public RecordWriter<ImmutableBytesWritable, Delete> getRecordWriter(
				TaskAttemptContext context) throws IOException,
				InterruptedException {
			return new TableRecordWriter(table);
		}

		@Override
		public void checkOutputSpecs(JobContext arg0) throws IOException,
				InterruptedException {
		}

		@Override
		public OutputCommitter getOutputCommitter(TaskAttemptContext arg0)
				throws IOException, InterruptedException {
			return new TableOutputCommitter();
		}

		@Override
		public void setConf(Configuration conf) {
//			if (table != null)
//				return;
			
			this.conf = HBaseConfiguration.create(conf);
			try {
				table = new HTable(this.conf, conf.get(TableInputFormat.INPUT_TABLE));
				this.table.setAutoFlush(false);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public Configuration getConf() {
			return conf;
		}

	}

	public static Job createSubmittableJob(Configuration conf,
			String tableName, Scan scan) throws IOException {
		conf = HBaseConfiguration.create(conf);
		Job job = new Job(conf, NAME + "_" + tableName + "_" + scan.hashCode());
		scan.setCaching(500);
		TableMapReduceUtil.initTableMapperJob(tableName, scan,
				TruncatorMapper.class, ImmutableBytesWritable.class,
				Delete.class, job, true);
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(Delete.class);
		job.setOutputFormatClass(TruncatorOutputFormat.class);
		job.setNumReduceTasks(0);
		return job;
	}
}
