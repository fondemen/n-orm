package com.googlecode.n_orm.hbase;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.ScannerTimeoutException;
import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Row;
import com.stumbleupon.async.Deferred;

final class CloseableIterator implements CloseableKeyIterator {
	private Scanner result;
	private Iterator<KeyValue> iterator;
	private final boolean sendValues;
	private final Class<? extends PersistingElement> clazz;
	private final MangledTableName table;
	private final String tablePostfix;
	private Constraint constraint;
	private int limit;
	private final Map<String, Field> families;
	private final Store store;
	private boolean reCreated = false;
	
	private byte[] currentKey = null;

	CloseableIterator(Store store, Class<? extends PersistingElement> clazz, MangledTableName table, String tablePostfix, Constraint constraint, int limit, Map<String, Field> families, Scanner r, boolean sendValues) {
		this.store = store;
		this.sendValues = sendValues;
		this.clazz = clazz;
		this.table = table;
		this.tablePostfix = tablePostfix;
		this.constraint = constraint;
		this.limit = limit;
		this.families = families;
		this.setResult(r);
	}
	
	private void setResult(Scanner result) {
		//Trying to close existing scanner
		if (this.result != null) {
			final Scanner res = this.result;
			new Thread(){
				@Override
				public void run() {
					res.notify(); /**** A REVOIR ***/
				}
			}.start();
		}
		this.result = result;
	}

	protected void handleProblem(RuntimeException x) {
		//Failure handling
		//Only one failure per scan accepted
		if (this.reCreated)
			throw x;
		this.reCreated = true;
		//Creating the iterator again, starting after the last scanned key
		if (this.currentKey != null) {
			this.constraint = new Constraint(Bytes.toString(currentKey) + Character.MIN_VALUE, this.constraint == null ? null : this.constraint.getEndKey());
		}
		if ((x.getCause() instanceof ScannerTimeoutException) || x.getMessage().contains(ScannerTimeoutException.class.getSimpleName())
				|| (x.getCause() instanceof UnknownScannerException) || x.getMessage().contains(UnknownScannerException.class.getSimpleName())) {
			Store.logger.warning("Got exception " + x.getMessage() + " ; consider lowering scanCahing or improve scanner timeout at the HBase level");
		} else {
			store.handleProblem(x, this.clazz, table, tablePostfix, this.families);
		}
		CloseableIterator newResult = (CloseableIterator) store.get(new MetaInformation().forClass(clazz).withColumnFamilies(families), table, constraint, limit, families == null ? null : families.keySet());
		this.setResult(newResult.result);
	}

	@Override
	public boolean hasNext() {
		try {
			boolean ret = false;
			if(this.result.nextRows()!=null){
				ret=true;
			}
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
			Deferred<ArrayList<ArrayList<KeyValue>>> current = this.result.nextRows();
			//this.currentKey = current.key(); // pour obtenir la row key.
			ArrayList<ArrayList<KeyValue>> i = current.join();
			
			this.limit--;
			this.reCreated = false;
			return new RowWrapper(current, this.sendValues);
			
		}
		catch (RuntimeException x) {
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
			 result.close(); // close the scanner before reaching the end of the key
		} catch (RuntimeException x) {
			store.handleProblem(x, this.clazz, table, tablePostfix, this.families);
		}
	}
}
