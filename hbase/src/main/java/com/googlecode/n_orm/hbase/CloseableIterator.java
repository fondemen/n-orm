package com.googlecode.n_orm.hbase;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

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
		return new RowWrapper(iterator.next(), this.sendValues);
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