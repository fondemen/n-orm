package com.googlecode.n_orm.hbase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.KeyValue;

import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.Row;

public class RowWrapper implements Row, Serializable {
	private static final long serialVersionUID = -3943538431236454382L;
	private final String key;
	private final ColumnFamilyData values;

	public RowWrapper(ArrayList<KeyValue> r) {
		this(r, true);
	}
	
	private Map<byte[], byte[]> fams=new TreeMap<byte[], byte[]>();
	
	public RowWrapper(ArrayList<KeyValue> r,  boolean sendValues) {
		Map<byte[],KeyValue>map = new HashMap<byte[],KeyValue>();
		for(KeyValue i :r)
			map.put(i.key(), i);
		this.key = Bytes.toString();
		if (sendValues) {
			this.values = new DefaultColumnFamilyData();
			for (Entry<byte[], KeyValue> famData : map.entrySet()) {
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
	public ColumnFamilyData getValues() {
		return this.values;
	}

}
