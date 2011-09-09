package com.googlecode.n_orm.hbase;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.googlecode.n_orm.storeapi.Row;

public class RowWrapper implements Row {
	private final String key;
	private final Map<String, Map<String, byte[]>> values;

	public RowWrapper(Result r) {
		this(r, true);
	}
		
	
	public RowWrapper(Result r, boolean sendValues) {
		this.key = Bytes.toString(r.getRow());
		if (sendValues) {
			this.values = new TreeMap<String, Map<String,byte[]>>();
			for (Entry<byte[], NavigableMap<byte[], byte[]>> famData : r.getNoVersionMap().entrySet()) {
				Map<String, byte[]> fam = new TreeMap<String, byte[]>();
				this.values.put(Bytes.toString(famData.getKey()), fam);
				for (Entry<byte[], byte[]> colData : famData.getValue().entrySet()) {
					fam.put(Bytes.toString(colData.getKey()), colData.getValue());
				}
			}
		} else
			this.values = null;
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public Map<String, Map<String, byte[]>> getValues() {
		return this.values;
	}

}
