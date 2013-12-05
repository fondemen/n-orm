package com.googlecode.n_orm.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.hbase.async.KeyValue;

public class HBaseDescriptor extends HashMap<MangledTableName, Object>{
	
	private MangledTableName tableName;
	private ArrayList<byte[]> qualifier;
	private ArrayList<byte[]> columnFamily;
	
	public MangledTableName getTableName() {
		return tableName;
	}

	public void setTableName(MangledTableName tableName) {
		this.tableName = tableName;
	}

	public ArrayList<byte[]> getQualifier() {
		return qualifier;
	}

	public void setQualifier(ArrayList<byte[]> qualifier) {
		this.qualifier = qualifier;
	}

	public ArrayList<byte[]> getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(ArrayList<byte[]> columnFamily) {
		this.columnFamily = columnFamily;
	}
	
	
	public void addQualifierToTable(MangledTableName tableName, ArrayList<byte[]> qualifier){
		this.put(tableName, qualifier);
	}
	public void addColumFamilyToTable(MangledTableName tableName, ArrayList<byte[]> family){
		this.put(tableName, qualifier);
	}

	public List<byte[]> getColumnFamily(ArrayList<KeyValue> kv){
		ArrayList<byte[]> alist = new ArrayList<byte[]>();
		for(KeyValue v: kv){
			alist.add(v.family());
		}
		return alist;
	}
	
	public List<byte[]> getQualifier(ArrayList<KeyValue> kv){
		ArrayList<byte[]> alist = new ArrayList<byte[]>();
		for(KeyValue v: kv){
			alist.add(v.qualifier());
		}
		return alist;
	}
	

}
