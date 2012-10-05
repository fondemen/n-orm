package com.googlecode.n_orm.cache.write;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.Transient;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.DelegatingStore;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Store;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;

public class WriteRetentionStore extends DelegatingStore {
	private static final byte[] DELETED_VALUE = new byte[0];
	
	private static final Map<Integer, Set<WriteRetentionStore>> knownStores = new HashMap<Integer, Set<WriteRetentionStore>>();
	
	// One should find the same WriteRetentionStore given a write retention time and a target store
	/**
	 * Returns a {@link WriteRetentionStore} with s as {@link DelegatingStore#getActualStore() delegate} ;
	 * in case s is already a {@link WriteRetentionStore} with a different {@link #getWriteRetentionMs()},
	 * return another {@link WriteRetentionStore} with same {@link DelegatingStore#getActualStore() delegate}
	 * as s.
	 * @param writeRetentionMs time during which updates are retended to delegate store
	 * @param s the actual store, or a {@link WriteRetentionStore} with delegating to the actual store
	 * @throws IllegalArgumentException if s is a delegation chain that contains a {@link WriteRetentionStore}
	 */
	public static WriteRetentionStore getWriteRetentionStore(long writeRetentionMs, Store s) {
		
		if (s instanceof WriteRetentionStore) {
			if (((WriteRetentionStore)s).getWriteRetentionMs() == writeRetentionMs)
				return (WriteRetentionStore) s;
			
			s = ((WriteRetentionStore)s).getActualStore();
		}
		
		//Checking whether a WriteRetentionStore exists in the delegating chain
		Store schk = s;
		while (schk instanceof DelegatingStore) {
			if (schk instanceof WriteRetentionStore)
				throw new IllegalArgumentException(s.toString() + " is already a write-retention store as it already delegates to " + schk + " ; cannot create write-retention store above it");
			schk = ((DelegatingStore)schk).getActualStore();
		}
		
		Set<WriteRetentionStore> res;
		// Return candidate if not exists
		WriteRetentionStore ret = new WriteRetentionStore(writeRetentionMs, s);
		int h = ret.hashCode();
		
		// Getting existing stores for this hash
		synchronized(knownStores) {
			res = knownStores.get(h);
			
			// No known store for this hash
			if (res == null) {
				res = new TreeSet<WriteRetentionStore>();
				knownStores.put(h, res);
			}
		}
		
		// Getting proper store from the set of known stores
		// or referencing the candidate if not found
		synchronized(res) {
			// Checking for existing store for this hash
			for (WriteRetentionStore kwrs : res) {
				if (kwrs.equals(ret))
					return kwrs;
			}
			
			// No store found ; referencing and then returning ret
			res.add(ret);
			return ret;
		}
	}

	private static class RequestIsOutException extends Exception {
		private static final long serialVersionUID = 4904072123077338091L;
	}

	private static class RowInTable {
		private final String table, id;
		private final int h;

		public RowInTable(String table, String id) {
			super();
			if (id == null || table == null)
				throw new IllegalArgumentException();
			this.table = table;
			this.id = id;
			this.h = this.computeHashCode();
		}

		@Override
		public int hashCode() {
			return this.h;
		}

		private int computeHashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + id.hashCode();
			result = prime * result + table.hashCode();
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			RowInTable other = (RowInTable) obj;
			if (h != other.h)
				return false;
			if (!id.equals(other.id))
				return false;
			if (!table.equals(other.table))
				return false;
			return true;
		}
	}

	/**
	 * Summarize requests for a given row in a given table. Thread-safely merges
	 * with another request.
	 */
	private static class StoreRequest implements Delayed {

		/**
		 * Data to be sent are all stored with a transaction index
		 */
		private final AtomicLong transactionDistributor = new AtomicLong(
				Long.MIN_VALUE);

		/**
		 * Whether this request was sent
		 */
		private volatile boolean sent = false;

		/**
		 * A lock to ensure this request is not updated while it's sent
		 */
		private final ReentrantReadWriteLock sendLock = new ReentrantReadWriteLock();

		/**
		 * The epoch date in nanoseconds at which this request should be sent to
		 * the data store
		 */
		private final long outDateNanos;

		/**
		 * When this element was deleted last.
		 */
		private ConcurrentSkipListSet<Long> deletions = new ConcurrentSkipListSet<Long>();

		private final AtomicReference<MetaInformation> meta;
		private final String table;
		private final String rowId;
		/**
		 * A map storing request data according to transaction number. Column
		 * can be a {@link byte[]}, a {@link WriteRetentionStore#DELETED_VALUE
		 * delete marker}, or a {@link Number}. If {@link byte[]} or
		 * {@link WriteRetentionStore#DELETED_VALUE delete marker}, only latest
		 * value is valid; if {@link Number}, datum corresponds to an increment
		 * an all values for all transaction should be summed to get the actual
		 * increment.
		 */
		private final ConcurrentMap<
			String /* family */,
			ConcurrentMap<
				String /* column */,
				ConcurrentNavigableMap<
					Long /* transaction id */,
					Object /* byte[] || Long (increment) || DELETED_VALUE */
		>>> elements;

		private StoreRequest(long outDateNanos, String table, String rowId) {
			super();
			this.outDateNanos = outDateNanos;
			this.meta = new AtomicReference<MetaInformation>();
			this.table = table;
			this.rowId = rowId;
			this.elements = new ConcurrentHashMap<String, ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>>>();
		}

		/**
		 * Starts an update ; {@link #doneUpdate()} must absolutely be
		 * eventually called.
		 * 
		 * @return transaction number
		 * @throws RequestIsOutException
		 *             in case this element is already being sending;
		 */
		private long startUpdate() throws RequestIsOutException {
			if (this.sent || !this.sendLock.readLock().tryLock())
				// Request is already sent or being sending
				throw new RequestIsOutException();

			if (this.sent) {
				this.sendLock.readLock().unlock();
				throw new RequestIsOutException();
			}

			return this.transactionDistributor.getAndIncrement();
		}

		/**
		 * Declares an update end.
		 * 
		 * @throws RequestIsOutException
		 */
		private void doneUpdate() throws RequestIsOutException {
			this.sendLock.readLock().unlock();
		}

		/**
		 * Marks this request as a delete.
		 * 
		 * @throws RequestIsOutException
		 *             in case this request is sent or about to be sent.
		 */
		public void delete(MetaInformation meta) throws RequestIsOutException {
			long transaction = this.startUpdate();
			try {
				this.deletions.add(transaction);
				if (this.deletions.size() > 1) {
					this.deletions.pollFirst();
				}

				this.addMeta(meta);

			} finally {
				this.doneUpdate();
			}
		}

		/**
		 * Thread-safe merge with new data.
		 * 
		 * @throws RequestIsOutException
		 *             in case this request is sent or about to be sent.
		 */
		public void update(MetaInformation meta, ColumnFamilyData changed,
				Map<String, Set<String>> removed,
				Map<String, Map<String, Number>> increments)
				throws RequestIsOutException {

			long transactionId = this.startUpdate();
			try {
				addMeta(meta);

				// Adding changes
				if (changed != null) {
					for (Entry<String, Map<String, byte[]>> famChanges : changed
							.entrySet()) {
						ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>> famData = this
								.getFamilyData(famChanges.getKey());
						for (Entry<String, byte[]> colChange : famChanges
								.getValue().entrySet()) {
							ConcurrentNavigableMap<Long, Object> columnData = this
									.getColumnData(famChanges.getKey(),
											famData, colChange.getKey());
							Object old = columnData.put(transactionId,
									colChange.getValue());
							// There must not have a previous put for the same transaction
							assert old == null;
							if (columnData.size() > 2) {
								// Other transaction already added some data in ;
								// removing the oldest one to avoid memory consumption
								// Not removing more as concurrent polls can happen here
								Entry<Long, Object> r = columnData
										.pollFirstEntry();
//								// Removed element must belong to an older transaction
								// Actually we cannot check that as the current transaction
								// might be removed by a newer one !
//								assert r == null
//										|| r.getKey().longValue() < transactionId;
								// Column data must not be left empty
								assert !columnData.isEmpty();
								// Removed element must be raw data (i.e. not a number from an increment)
								assert r == null
										|| r.getValue() instanceof byte[];
							}
						}
					}
				}

				// Adding increments
				if (increments != null) {
					for (Entry<String, Map<String, Number>> famincrements : increments
							.entrySet()) {
						ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>> famData = this
								.getFamilyData(famincrements.getKey());
						for (Entry<String, Number> colIncrements : famincrements
								.getValue().entrySet()) {
							ConcurrentNavigableMap<Long, Object> columnData = this
									.getColumnData(famincrements.getKey(),
											famData, colIncrements.getKey());
							Object old = columnData.put(transactionId,
									colIncrements.getValue());
							// There must not have a previous put for the same transaction
							assert old == null;
							// Not removing older entries as the actual output is
							// the sum of the values for the different transactions
						}
					}
				}

				// Adding deletion markers
				if (removed != null) {
					for (Entry<String, Set<String>> famRemoves : removed
							.entrySet()) {
						ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>> famData = this
								.getFamilyData(famRemoves.getKey());
						for (String colRemoved : famRemoves.getValue()) {
							ConcurrentNavigableMap<Long, Object> columnData = this
									.getColumnData(famRemoves.getKey(),
											famData, colRemoved);
							Object old = columnData.put(transactionId,
									DELETED_VALUE);
							// There must not have a previous put for the same transaction
							assert old == null;
							// There might be an inconsistency in input parameters
							if (columnData.size() > 2 && old != null) {
								// Other transaction already added some data in ;
								// removing the oldest one to avoid memory consumption
								// Not removing more as concurrent polls can happen here
								Entry<Long, Object> r = columnData
										.pollFirstEntry();
//								// Removed element must belong to an older transaction
								// Actually we cannot check that as the current transaction
								// might be removed by a newer one !
//								assert r == null
//										|| r.getKey().longValue() < transactionId;
								// Column data must not be left empty
								assert !columnData.isEmpty();
							}
						}
					}
				}
			} finally {
				this.doneUpdate();
			}
		}

		/**
		 * Adding meta information
		 */
		private void addMeta(MetaInformation meta) {
			if (meta != null) {
				// Setting meta in case null
				if (!this.meta.compareAndSet(null, meta)) {
					// Or integrate it if it already exists
					this.meta.get().integrate(meta);
				}
			}
		}

		/**
		 * Gets family data for this object. Creates and integrates it if
		 * necessary. Thread-safe.
		 */
		private ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>> getFamilyData(
				String family) {
			ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>> ret = this.elements
					.get(family);
			if (ret == null) {
				ret = new ConcurrentHashMap<String, ConcurrentNavigableMap<Long, Object>>();
				ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>> put = this.elements
						.putIfAbsent(family, ret);
				if (put != null)
					ret = put;
			}
			return ret;
		}

		/**
		 * Gets column data for this object. Creates and integrates it if
		 * necessary. Thread-safe.
		 */
		private ConcurrentNavigableMap<Long, Object> getColumnData(
				String family,
				ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>> familyData,
				String column) {
			assert this.elements.get(family) == familyData;
			ConcurrentNavigableMap<Long, Object> ret = familyData.get(column);
			if (ret == null) {
				ret = new ConcurrentSkipListMap<Long, Object>();
				ConcurrentNavigableMap<Long, Object> put = familyData
						.putIfAbsent(column, ret);
				if (put != null)
					ret = put;
			}
			return ret;
		}

		/**
		 * Cleans up data a little bit data before this transaction in order to
		 * free memory if transaction can be divided by 10.
		 */
		private void cleanup(long transaction) {
			if (transaction % 10 == 0) {
				for (Entry<String, ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>>> famData : this.elements
						.entrySet()) {
					for (Entry<String, ConcurrentNavigableMap<Long, Object>> colData : famData
							.getValue().entrySet()) {
						Iterator<Long> it = colData.getValue().keySet()
								.iterator();
						while (it.hasNext()) {
							long dataTransaction = it.next();
							if (dataTransaction < transaction)
								it.remove();
							else
								break;
						}
					}
				}
			}
		}

		@Override
		public int compareTo(Delayed d) {
			if (this == d)
				return 0;
			StoreRequest o = (StoreRequest) d;
			if (this.outDateNanos == o.outDateNanos) {
				return (this.table + this.rowId).compareTo(o.table + o.rowId);
			} else if (this.outDateNanos < o.outDateNanos)
				return -1;
			else
				// if (this.outDateNanos > o.outDateNanos)
				return 1;
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return unit.convert(this.outDateNanos - System.nanoTime(),
					TimeUnit.NANOSECONDS);
		}

		/**
		 * Sending this request. Waits for current updates to be done.
		 * 
		 * @param store
		 *            the store to which send the request
		 * @param sender
		 *            the executor for sending the request
		 * @param counter
		 *            a counter to be decremented when the request is sent
		 */
		public void send(final Store store, ExecutorService sender,
				final AtomicInteger counter) {
			counter.incrementAndGet();
			this.sendLock.writeLock().lock();
			long lastTransaction;
			try {
				this.sent = true;
				// Immediately letting other threads know that it's over
				lastTransaction = transactionDistributor.get();
			} finally {
				this.sendLock.writeLock().unlock();
			}
			// After this line, this request cannot be changed by any thread !

			Long lastDeletion = this.deletions.isEmpty() ? null : this.deletions.last();

			long lastStore = Long.MIN_VALUE;

			// Grabbing changed values.
			final ColumnFamilyData changes = new DefaultColumnFamilyData();
			final Map<String, Set<String>> removed = new TreeMap<String, Set<String>>();
			final Map<String, Map<String, Number>> increments = new TreeMap<String, Map<String, Number>>();
			for (Entry<String, ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>>> famData : this.elements
					.entrySet()) {
				for (Entry<String, ConcurrentNavigableMap<Long, Object>> colData : famData
						.getValue().entrySet()) {
					Entry<Long, Object> lastEntry = colData.getValue()
							.lastEntry();
					Long transaction = lastEntry.getKey();
					lastStore = Math.max(lastStore, transaction);
					// Considering only those values after last deletion
					if (lastDeletion == null || lastDeletion < transaction) {
						Object latest = lastEntry.getValue();
						if (latest instanceof byte[]) {
							if (latest == DELETED_VALUE) {
								// Column should be deleted
								Set<String> remCols = removed.get(famData
										.getKey());
								if (remCols == null) {
									remCols = new TreeSet<String>();
									removed.put(famData.getKey(), remCols);
								}
								remCols.add(colData.getKey());
							} else {
								// Column should be changed
								Map<String, byte[]> chgCols = changes
										.get(famData.getKey());
								if (chgCols == null) {
									chgCols = new TreeMap<String, byte[]>();
									changes.put(famData.getKey(), chgCols);
								}
								chgCols.put(colData.getKey(), (byte[]) latest);
							}
						} else {
							assert latest instanceof Number;
							// Column should be incremented
							// Summing all values to know the actual increment
							long sum = 0;
							for (Entry<Long, Object> incr : colData.getValue()
									.entrySet()) {
								Object val = incr.getValue();
								// Just in case datum was deleted meanwhile
								if (val == DELETED_VALUE)
									sum = 0;
								else
									sum += ((Number) val).longValue();
							}

							Map<String, Number> incCols = increments
									.get(famData.getKey());
							if (incCols == null) {
								incCols = new TreeMap<String, Number>();
								increments.put(famData.getKey(), incCols);
							}
							incCols.put(colData.getKey(), sum);
						}
					}
				}
			}

			// Checking that no other thread has indeed made another request
			// meanwhile
			assert lastTransaction == this.transactionDistributor.get() : "A request about to be sent has changed !";

			// Sending using the executor
			// Checking whether it's a store or a delete
			if (lastDeletion == null || lastDeletion < lastStore) {
				sender.execute(new Runnable() {

					@Override
					public void run() {
						try {
							store.storeChanges(meta.get(), table, rowId,
									changes, removed, increments);
						} catch (RuntimeException x) {
							x.printStackTrace();
							throw x;
						} finally {
							counter.decrementAndGet();
						}
					}
				});
			} else {
				sender.execute(new Runnable() {

					@Override
					public void run() {
						try {
							store.delete(meta.get(), table, rowId);
						} catch (RuntimeException x) {
							x.printStackTrace();
							throw x;
						} finally {
							counter.decrementAndGet();
						}
					}
				});
			}
		}

	}

	private class EvictionThread extends Thread {
		private final ExecutorService sender = Executors.newCachedThreadPool();

		private volatile boolean alreadyStarted = false;
		private volatile boolean running = true;

		@Override
		public synchronized void start() {
			if (this.alreadyStarted)
				return;
			this.alreadyStarted = true;
			super.start();
			// Registering last chance to evict all the list of elements in the
			// store
			Runtime.getRuntime().addShutdownHook(new Thread() {

				@Override
				public void run() {
					try {
						// Prevents new requests to be recorded
						stopped = true;
						// Stops the queue dequeue
						running = false;
						// Stop waiting for the queue
						EvictionThread.this.interrupt();

						// Sending pending requests using one single thread
						ExecutorService sender = Executors
								.newSingleThreadExecutor();
						Iterator<StoreRequest> it = writeQueue.iterator();
						while (it.hasNext()) {
							it.next()
									.send(getActualStore(),
											sender,
											WriteRetentionStore.this.requestsBeingSending);
							it.remove();
						}

						// Waiting for all to be sent
						EvictionThread.this.sender.shutdown();
						sender.shutdown();

						EvictionThread.this.sender.awaitTermination(
								Long.MAX_VALUE, TimeUnit.MILLISECONDS);
						sender.awaitTermination(Long.MAX_VALUE,
								TimeUnit.MILLISECONDS);

					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}

			});
		}

		@Override
		public void run() {
			while (this.running) {
				try {
					StoreRequest r = writeQueue.take();
					writesByRows.remove(new RowInTable(r.table, r.rowId));
					r.send(getActualStore(), this.sender,
							WriteRetentionStore.this.requestsBeingSending);
				} catch (InterruptedException e) {
				}
			}
		}

	}

	private abstract static class Operation {
		public abstract void run(StoreRequest req) throws RequestIsOutException;
	}
	
	
	
	//Whether this store was already started
	private AtomicBoolean started = new AtomicBoolean(false);

	// Time before request is sent
	private long writeRetentionMs;

	// Waiting queue
	private final DelayQueue<StoreRequest> writeQueue;
	// Index for requests according to the row
	@Transient
	private final ConcurrentMap<RowInTable, StoreRequest> writesByRows;

	private volatile boolean stopped = false;

	// Number of requests currently being sending
	private final AtomicInteger requestsBeingSending = new AtomicInteger();
	
	private WriteRetentionStore(long writeRetentionMs, Store s) {
		super(s);
		this.writeRetentionMs = writeRetentionMs;
		this.writeQueue = new DelayQueue<StoreRequest>();
		this.writesByRows = new ConcurrentHashMap<RowInTable, StoreRequest>();
	}

	/**
	 * Minimum time in ms during which and update (i.e. a
	 * {@link Store#storeChanges(MetaInformation, String, String, ColumnFamilyData, Map, Map)
	 * store} or a {@link Store#delete(MetaInformation, String, String) delete})
	 * are retained before being sent to the {@link #getActualStore() actual
	 * store}. During this time, updates are merged to dramatically reduce
	 * number of updates on a same row.
	 */
	public long getWriteRetentionMs() {
		return writeRetentionMs;
	}

	@Override
	public void start() throws DatabaseNotReachedException {
		if (started.compareAndSet(false, true)) {
			new EvictionThread().start();
			super.start();
		}
	}

	@Override
	public void delete(final MetaInformation meta, String table, String id)
			throws DatabaseNotReachedException {
		if (stopped) {
			this.getActualStore().delete(meta, table, id);
			return;
		}

		this.runLater(table, id, new Operation() {

			@Override
			public void run(StoreRequest req) throws RequestIsOutException {
				req.delete(meta);
			}
		});
	}

	@Override
	public void storeChanges(final MetaInformation meta, String table,
			String id, final ColumnFamilyData changed,
			final Map<String, Set<String>> removed,
			final Map<String, Map<String, Number>> increments)
			throws DatabaseNotReachedException {
		if (stopped) {
			this.getActualStore().storeChanges(meta, table, id, changed,
					removed, increments);
			return;
		}

		this.runLater(table, id, new Operation() {

			@Override
			public void run(StoreRequest req) throws RequestIsOutException {
				req.update(meta, changed, removed, increments);
			}
		});
	}

	/**
	 * Plans a request sending. Sends immediately in case this thread in case we
	 * are about to shutdown.
	 * 
	 * @param table
	 *            table of the element
	 * @param id
	 *            id of the element
	 * @param r
	 *            the operation to be performed
	 */
	private void runLater(String table, String id, Operation r) {
		RowInTable element = new RowInTable(table, id);
		boolean tbd;
		do {
			StoreRequest req = null;//this.writesByRows.get(element);
			if (req == null) {
				req = new StoreRequest(
						TimeUnit.NANOSECONDS.convert(this.writeRetentionMs,
								TimeUnit.MILLISECONDS) + System.nanoTime(), table, id);
				StoreRequest tmp = this.writesByRows.putIfAbsent(element, req);
				if (tmp == null) {
					// req was added ; should also be put in the delay queue
					this.writeQueue.put(req);
				} else {
					// Another thread added request for this element before us
					req = tmp;
				}
			}
			try {
				r.run(req);
				// Request is planned and merged ; leaving the infinite loop
				tbd = false;
			} catch (RequestIsOutException x) {
				// We've tried to update a request that went out meanwhile
				// retrying eventually
				tbd = true;
				Thread.yield();
			}
		} while (tbd);
	}

	/**
	 * The approximate number of pending write requests.
	 */
	public int getPendingRequests() {
		return this.writeQueue.size() + requestsBeingSending.get();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.getActualStore().hashCode();
		result = prime * result
				+ (int) (writeRetentionMs ^ (writeRetentionMs >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WriteRetentionStore other = (WriteRetentionStore) obj;
		if (writeRetentionMs != other.writeRetentionMs)
			return false;
		if (!this.getActualStore().equals(other.getActualStore()))
			return false;
		return true;
	}
}
