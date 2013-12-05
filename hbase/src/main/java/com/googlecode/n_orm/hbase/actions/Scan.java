package com.googlecode.n_orm.hbase.actions;

import java.util.ArrayList;

import org.hbase.async.Scanner;


public class Scan {
	private Integer caching;
	private byte[] startRow, stopRow;
	private ArrayList<byte[]> families =new ArrayList<byte[]>();
	private Scanner s;
	
	public Scan(){}

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

}
