package com.googlecode.n_orm.hbase;

import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.Row;

final class CloseableIterator implements CloseableKeyIterator {
	private ResultScanner result;
	private Iterator<Result> iterator;
	private final boolean sendValues;
	private final String table;
	private Constraint constraint;
	private int limit;
	private final Set<String> families;
	private final Store store;
	private boolean reCreated = false;
	
	private byte[] currentKey = null;

	CloseableIterator(Store store, String table, Constraint constraint, int limit, Set<String> families, ResultScanner res, boolean sendValues) {
		this.store = store;
		this.sendValues = sendValues;
		this.table = table;
		this.constraint = constraint;
		this.limit = limit;
		this.families = families;
		this.setResult(res);
	}
	
	private void setResult(ResultScanner result) {
		//Trying to close existing scanner
		if (this.result != null) {
			final ResultScanner res = this.result;
			new Thread(){
				@Override
				public void run() {
					res.close();
				}
			}.start();
		}
		this.result = result;
		this.iterator = result.iterator();
	}

	protected void handleProblem(RuntimeException x) {
		//Failure handling
		//Only one failure per scan (next or hasNext) accepted
		if (this.reCreated)
			throw x;
		this.reCreated = true;
		//Creating the iterator again, starting after the last scanned key
		if (this.currentKey != null) {
			this.constraint = new Constraint(Bytes.toString(currentKey) + Character.MIN_VALUE, this.constraint == null ? null : this.constraint.getEndKey());
		}
		
		store.handleProblem(store.createLazyAdmin(), x, table, families == null ? null : families.toArray(new String[families.size()]));
		CloseableIterator newResult = (CloseableIterator) store.get(table, constraint, limit, families);
		this.setResult(newResult.result);
	}

	@Override
	public boolean hasNext() {
		try {
			boolean ret = iterator.hasNext();
			this.reCreated = false;
			return ret;
		} catch (RuntimeException x) {
			this.handleProblem(x);
			return hasNext();
		}
	}

	@Override
	public Row next() {
		try {
			Result current = iterator.next();
			this.currentKey = current.getRow();
			this.limit--;
			this.reCreated = false;
			return new RowWrapper(current, this.sendValues);
		} catch (RuntimeException x) {
			this.handleProblem(x);
			return next();
		}
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
		try {
			result.close();
		} catch (RuntimeException x) {
			store.handleProblem(store.createLazyAdmin(), x, table, families == null ? null : families.toArray(new String[families.size()]));
		}
	}
}