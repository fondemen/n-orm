package com.googlecode.n_orm.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.ScanHandler;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;

public class LocalInputFormat extends
		TableInputFormat implements Configurable {
	private LocalFormat localFormat = new LocalFormat();
	private TableRecordReader inputReader;

	@Override
	public void setConf(Configuration conf) {
		if (inputReader != null)
			return;

		super.setConf(conf);
		localFormat.setConf(conf);
		try {
			this.initializeTable(
					ConnectionFactory.createConnection(conf),
					localFormat.getTable());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		this.setScan(ScanHandler.getScan(conf));
	}

	@Override
	protected void initializeTable(Connection connection, TableName tableName)
			throws IOException {
		TableName ltable = localFormat.getTable();
		if (ltable != null)
			super.initializeTable(connection, ltable);
		else
			super.initializeTable(connection, tableName);
	}
}
