package com.googlecode.n_orm.hbase;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.Row;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;

public class RowWrapper implements Row, Serializable {
	private static final long serialVersionUID = -3943538431236454382L;
	private final String key;
	private final ColumnFamilyData values;

	public RowWrapper(Result r) {
		this(r, true);
	}
		
	
	public RowWrapper(Result r, boolean sendValues) {
		this.key = Bytes.toString(r.getRow());
		if (sendValues) {
			this.values = new DefaultColumnFamilyData();

			for (Cell kv : r.rawCells()) {
				String familyName = Bytes.toString(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength());
				Map<String, byte[]> fam = this.values.get(familyName);
				if (fam == null) {
					fam = new TreeMap<String, byte[]>();
					this.values.put(familyName, fam);
				}
				fam.put(Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()),
						CellUtil.cloneValue(kv));
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
