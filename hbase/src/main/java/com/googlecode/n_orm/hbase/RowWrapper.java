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
	
	//private Map<byte[], byte[]> fams=new TreeMap<byte[], byte[]>();
	
	public RowWrapper(ArrayList<KeyValue> r,  boolean sendValues) {
		this.key = Bytes.toString(r.get(0).key());
		
		if (sendValues) {
			this.values = new DefaultColumnFamilyData();
			for(KeyValue kv: r){
				Map<String, byte[]> fam = new TreeMap<String, byte[]>();
				fam.put(Bytes.toString(kv.qualifier()), kv.value());
				this.values.put(Bytes.toString(kv.family()), fam);
				}
			}
		 else
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
