package com.googlecode.n_orm.hbase.mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LocalOutputFormat<T> extends
		OutputFormat<ImmutableBytesWritable, T> implements Configurable {
	public static final String OUTPUTWRITER_CLASS = "outputwriter-class";
	private LocalFormat localFormat = new LocalFormat();
	private LocalTableRecordWriter<T> outputWriter;

	@Override
	public RecordWriter<ImmutableBytesWritable, T> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return this.outputWriter;
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
		if (outputWriter != null)
			return;

		localFormat.setConf(conf);
		
		try {
			@SuppressWarnings("unchecked")
			Class<? extends LocalTableRecordWriter<T>> outputWriterClass = (Class<? extends LocalTableRecordWriter<T>>) LocalOutputFormat.class.getClassLoader().loadClass(conf.get(OUTPUTWRITER_CLASS));
			Constructor<? extends LocalTableRecordWriter<T>> outputWriterConstr = outputWriterClass.getConstructor(HTable.class);
			outputWriter = outputWriterConstr.newInstance(localFormat.getTable());
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public Configuration getConf() {
		return localFormat.getConf();
	}
}
