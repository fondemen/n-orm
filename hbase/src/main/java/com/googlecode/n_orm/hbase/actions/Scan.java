package com.googlecode.n_orm.hbase.actions;

import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Result;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import com.stumbleupon.async.Deferred;

public class Scan {
	private int caching;
	private byte[] startRow, stopRow;
	private byte[] family;
	private byte[] qualifier;
	private byte[][] qualifiers;
	private  Scanner s;

	
	public Scan() {
		this.s=s;
		this.caching=0;
		
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
		this.startRow=startRow;
	
	}

	public void setStopRow(byte[] stopKey) {
		this.stopRow=stopRow;
	}

	public void setFamily(byte[] fam) { // specify a particular column family to scan
		this.family=fam;
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
	public byte[] getCurrentKey(){
	
		return this.s.getCurrentKey();
		
	}
	public byte[] getQualifier() {
		return this.qualifier;
	}
	public void setQualifiers(byte[][] qualifiers){
		this.qualifiers=qualifiers;
	}
	public byte[][] getQualifiers(){
		return this.qualifiers;
	}
	
	

}
