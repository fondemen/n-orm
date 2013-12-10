package com.googlecode.n_orm.hbase.actions;

import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Result;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import com.stumbleupon.async.Deferred;


public class Scan {
	private Integer caching;
	private byte[] startRow, stopRow;
	private int length ;
	private ArrayList<byte[]> families;
	private Scanner s;
	
	public Scan(){
		families =new ArrayList<byte[]>();
	}

	public void setStartRow(byte[] bytes) {
		this.startRow=bytes;
	}

	public void setStopRow(byte[] endb) {
		this.stopRow=endb;
	}

	public void addFamily(byte[] fam) {
		families.add(fam);
	}

	public void setFilter(String filter) {
		s.setKeyRegexp(filter);
	}

	public void setCaching(Integer caching) {
		this.caching=caching;
		
	}

	public int getCaching() {
		return  this.caching;
	}

	public void close() {	
	}

	public ArrayList<KeyValue> next(int nbRows) {
		
		return null;
	}

	public Object next() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setCacheBlocks(boolean b) {
		// TODO Auto-generated method stub	
	}

}
