package com.googlecode.n_orm.hbase.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.hibernate.management.impl.BeanUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.aspectj.lang.Aspects;
import org.slf4j.Logger;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.StorageManagement;
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
		//Disabling speculative execution
	    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
	    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		return conf;
	}
	
	public static void prepareJob(Job job, Scan scan, Store store) throws IOException {
		if (store.isMapRedSendHBaseJars())
			TableMapReduceUtil.addDependencyJars(job.getConfiguration(), org.apache.zookeeper.ZooKeeper.class, HTable.class, HBaseAdmin.class, HTablePool.class, HColumnDescriptor.class, Scan.class, Increment.class);
		if (store.isMapRedSendNOrmJars())
			TableMapReduceUtil.addDependencyJars(job.getConfiguration(), Store.class, StorageManagement.class, Aspects.class, Ehcache.class, Logger.class, org.apache.log4j.Logger.class, BeanUtils.class);
		
		scan.setCaching(store.getMapRedScanCaching());
		scan.setCacheBlocks(false);
		
		job.setInputFormatClass(LocalInputFormat.class);
		if (job.getConfiguration().get(LocalOutputFormat.OUTPUTWRITER_CLASS) != null)
			job.setOutputFormatClass(LocalOutputFormat.class);
		else
			job.setOutputFormatClass(NullOutputFormat.class);
		TableMapReduceUtil.setScannerCaching(job, 500);
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
