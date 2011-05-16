package com.googlecode.n_orm.hbase;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Row;

final class CloseableIterator implements CloseableKeyIterator {
	private final ResultScanner result;
	private final Iterator<Result> iterator;
	private final boolean sendValues;

	CloseableIterator(ResultScanner res, boolean sendValues) {
		this.result = res;
		this.sendValues = sendValues;
		this.iterator = res.iterator();
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public Row next() {
		final Result r = iterator.next();
		final String key = Bytes.toString(r.getRow());
		final Map<String, Map<String, byte[]>> vals = this.sendValues ? new TreeMap<String, Map<String,byte[]>>() : null;
		if (this.sendValues) {
			for (Entry<byte[], NavigableMap<byte[], byte[]>> famData : r.getNoVersionMap().entrySet()) {
				Map<String, byte[]> fam = new TreeMap<String, byte[]>();
				vals.put(Bytes.toString(famData.getKey()), fam);
				for (Entry<byte[], byte[]> colData : famData.getValue().entrySet()) {
					fam.put(Bytes.toString(colData.getKey()), colData.getValue());
				}
			}
		}
		return new Row() {
			
			@Override
			public Map<String, Map<String, byte[]>> getValues() {
				return vals;
			}
			
			@Override
			public String getKey() {
				return key;
			}
		};
	}

	@Override
	public void remove() {
		throw new IllegalStateException(
				"Cannot remove key from a result set.");
	}

	@Override
	protected void finalize() throws Throwable {
		this.close();
		super.finalize();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.norm.hbase.CloseableIterator#close()
	 */
	@Override
	public void close() {
		result.close();
	}
}