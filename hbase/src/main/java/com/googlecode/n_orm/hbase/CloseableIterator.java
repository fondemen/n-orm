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
	private Scanner s;
	private final boolean sendValues;
	private final Class<? extends PersistingElement> clazz;
	private final MangledTableName table;
	private final String tablePostfix;
	private Constraint constraint;
	private int limit; 
	private final Map<String, Field> families;
	private final Store store;
	private boolean reCreated = false;
	private Iterator<ArrayList<KeyValue>> it;
	private Deferred<ArrayList<ArrayList<KeyValue>>> defer;
	ArrayList<ArrayList<KeyValue>> nextBlock;
	private int scanCaching;


	private ArrayList<ArrayList<KeyValue>>result;
	private int counter;
	
	
	private byte[] currentKey = null;

	CloseableIterator(Store store, Class<? extends PersistingElement> clazz, MangledTableName table, String tablePostfix, Constraint constraint, int limit, Map<String, Field> families, Scanner r, boolean sendValues, int ScanCaching) {
		this.store = store;
		this.sendValues = sendValues;
		this.clazz = clazz;
		this.table = table;
		this.tablePostfix = tablePostfix;
		this.constraint = constraint;
		this.limit = limit;
		this.families = families;
		this.scanCaching=ScanCaching;
		this.defer=r.nextRows(ScanCaching);
		this.counter=ScanCaching/2;
		this.setResult(r);

		
	}
	
	private void setResult(Scanner s) {
		//Trying to close existing scanner
		if (this.s != null) {
			final Scanner res = this.s;
			new Thread(){
				@Override
				public void run() {
					res.close();
				}
			}.start();
		}
		this.s = s;
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
		this.setResult(newResult.s);
	}
	
	@Override
	/*
	 * ROW: ArrayList<KeyValue>*/
	public Row next() {
		if(this.hasNext()){
			if(this.counter!=0){
				this.counter--;
				this.limit--;
				this.reCreated=false;
				return new RowWrapper( it.next(), sendValues);
			}
			else{
				try {
					this.limit--;
					this.reCreated=true;
					new CloseableIterator(store, clazz, table, tablePostfix, constraint, limit, families, s, sendValues, scanCaching);
					
					if(this.s!=null){
						Deferred<ArrayList<ArrayList<KeyValue>>> object = s.nextRows(scanCaching);
						 this.nextBlock = object.join();
						 
					}			
					return new RowWrapper( it.next(), sendValues);
				} catch (RuntimeException e) {
					this.handleProblem(e);
				} catch (Exception e) {
					
					e.printStackTrace();
				}
				//new CloseableIterator(store, clazz, table, tablePostfix, constraint, limit, families, s, sendValues, scanCaching);
				
			}
		}
		else{
			this.result=this.nextBlock;
			next();
			
		}
		
		return next();
		
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
			 s.close(); // close the scanner before reaching the end of the key
		} catch (RuntimeException x) {
			store.handleProblem(x, this.clazz, table, tablePostfix, this.families);
		}
	}

	@Override
	public boolean hasNext() {	
	try {
		this.result = this.defer.join();
		 this.it=result.iterator();
		 boolean ret = this.it.hasNext();
		 return ret;
	} catch (RuntimeException x) {
		this.handleProblem(x);
	} catch (Exception e) {
		e.printStackTrace();
	}
		return hasNext();
	}
	
	public int getScanCaching() {
		return scanCaching;
	}

	public void setScanCaching(int scanCaching) {
		this.scanCaching = scanCaching;
	}
}
