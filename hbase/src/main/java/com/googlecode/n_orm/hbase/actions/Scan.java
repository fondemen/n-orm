package com.googlecode.n_orm.hbase.actions;

import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Result;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import com.stumbleupon.async.Deferred;

public class Scan {
	private Integer caching;
	private byte[] startRow, stopRow;
	private byte[] family;
	private Scanner s;

	public Scan(Scanner s) {
		this.s = s;
	}
	public Scan() {
	}

	public byte[] getStartRow() {
		return startRow;
	}

	public byte[] getStopRow() {
		return stopRow;
	}

	public Scanner getScanner() {
		return s;
	}

	public void setScanner(Scanner s) {
		this.s = s;
	}

	public void setStartRow(byte[] startRow) {
		this.s.setStartKey(startRow);
	}

	public void setStopRow(byte[] stopKey) {
		this.s.setStopKey(stopKey);
	}

	public void setFamily(byte[] fam) { // specify a particular column family to scan
		this.s.setFamily(fam);
	}
	public void addFamily(byte[] family){ // Get all the column for the specify family
		this.s.setFamily(family);
	}

	public void setFilter(String filter) {
		s.setKeyRegexp(filter);
	}

	public void setCaching(Integer caching) {
		this.caching = caching;
	}

	public int getCaching() {
		return this.caching;
	}

	public void close() {
		this.s.close();
	}

	public Deferred<ArrayList<ArrayList<KeyValue>>> next(int nbRows) {
		return this.s.nextRows(nbRows);
	}

	public Deferred<ArrayList<ArrayList<KeyValue>>> next() {
		return this.s.nextRows();
	}

	public void setCacheBlocks(boolean b) {
		this.s.setServerBlockCache(b);
	}

	public byte[] getFamily() {
		return this.family;
	}
	public byte[] getKey(){
	
		return this.s.getCurrentKey();
		
	}
	

}
