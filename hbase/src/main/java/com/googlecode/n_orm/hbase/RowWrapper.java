package com.googlecode.n_orm.hbase;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.KeyValue;

import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.Row;

public class RowWrapper implements Row, Serializable {
	private static final long serialVersionUID = -3943538431236454382L;
	private final String key;
	private final ColumnFamilyData values;

	public RowWrapper(KeyValue r) {
		this(r, true);
	}
		
	// on n'a pas un ensemble de valeurs mais une seule valeur
	public RowWrapper(KeyValue current, boolean sendValues) {
		this.key = Bytes.toString(current.key());
		if (sendValues) {
			this.values = new DefaultColumnFamilyData();
			for (Entry<byte[], NavigableMap<byte[], byte[]>> famData : current.getNoVersionMap().entrySet()) {
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
