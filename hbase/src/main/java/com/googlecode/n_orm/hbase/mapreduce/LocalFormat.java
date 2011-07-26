package com.googlecode.n_orm.hbase.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.googlecode.n_orm.hbase.Store;

public class LocalFormat {
	public static final String STORE_REF = "store-props";
	
	public static final Configuration prepareConf(Store s, Class<?> outputWriterClass) throws IOException {
		Configuration conf = HBaseConfiguration.create(s.getConf());
		Properties props = s.getLaunchProps();
		StringWriter swp = new StringWriter();
		props.store(swp, null);
		String sp = swp.toString();
		conf.set(LocalFormat.STORE_REF, sp);
		if (outputWriterClass != null)
			conf.set(LocalOutputFormat.OUTPUTWRITER_CLASS, outputWriterClass.getName());
		return conf;
	}
	
	public static void prepareJob(Job job) {
		job.setJarByClass(Store.class);
		job.setInputFormatClass(LocalInputFormat.class);
		if (job.getConfiguration().get(LocalOutputFormat.OUTPUTWRITER_CLASS) != null)
			job.setOutputFormatClass(LocalOutputFormat.class);
		else
			job.setOutputFormatClass(NullOutputFormat.class);
	}

	protected HTable table;
	private Configuration conf;
	
	public HTable getTable() {
		return table;
	}

	public Configuration getConf() {
		return conf;
	}
	
	protected void setConf(Configuration conf) {

		Store hstore = null;
		try {
			Properties props = null;
			StringReader srp = new StringReader(conf.get(STORE_REF));
			props = new Properties();
			props.load(srp);
			hstore = Store.getKnownStore(props);
			this.conf = hstore.getConf();
		} catch (Exception e1) {
			e1.printStackTrace();
			this.conf = HBaseConfiguration.create(conf);
		}

		try {
			table = new HTable(this.conf,
					conf.get(TableInputFormat.INPUT_TABLE));
			this.table.setAutoFlush(false);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
}
