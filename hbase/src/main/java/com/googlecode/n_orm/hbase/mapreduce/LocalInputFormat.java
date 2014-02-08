package com.googlecode.n_orm.hbase.mapreduce;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.ScanHandler;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;

import com.googlecode.n_orm.hbase.MangledTableName;

public class LocalInputFormat extends
		TableInputFormat implements Configurable {
	private LocalFormat localFormat = new LocalFormat();
	private TableRecordReader inputReader;

	@Override
	public void setConf(Configuration conf) {
		if (inputReader != null)
			return;

		localFormat.setConf(conf);
		this.setMangledTableName(localFormat.getTable());
		this.setScan(ScanHandler.getScan(conf));
	}

	private void setMangledTableName(MangledTableName table) {
		// TODO Auto-generated method stub
		MangledTableName tableName=localFormat.getTable();
		if(tableName!=null){
			setMangledTableName(tableName);}
		else{
			setMangledTableName(table);
		}
	}

	/*@Override
	protected void setHTable(HTable table) {
		HTable ltable = localFormat.getTable();
		if (ltable != null)
			super.setHTable(ltable);
		else
			super.setHTable(table);
	}*/
}
