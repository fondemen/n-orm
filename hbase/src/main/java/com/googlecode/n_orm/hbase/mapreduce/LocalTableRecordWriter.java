package com.googlecode.n_orm.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public abstract class LocalTableRecordWriter<T> extends
		RecordWriter<ImmutableBytesWritable, T> {

	private final HTable table;

	public LocalTableRecordWriter(HTable table) {
		this.table = table;
	}

	public HTable getTable() {
		return table;
	}

	@Override
	public void close(TaskAttemptContext ctx) throws IOException,
			InterruptedException {
		table.flushCommits();
	}

}