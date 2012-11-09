package com.googlecode.n_orm.cache.write;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.Transient;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.DelegatingStore;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Store;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;

public class WriteRetentionStore extends DelegatingStore {
	private static final byte[] DELETED_VALUE = new byte[0];
	
	// Event queue
	private static final DelayQueue<StoreRequest> writeQueue = new DelayQueue<StoreRequest>();
	
	// Number of requests currently being sending
	private static final AtomicInteger requestsBeingSending = new AtomicInteger();
	
	// Whether we are being stopping (JVM shutdown)
	private static volatile boolean shutdown = false;

	// Thread responsible for dequeue
	private static final EvictionThread evictionThread;
	
	// Known stores
	private static final Map<Integer, Collection<WriteRetentionStore>> knownStores = new HashMap<Integer, Collection<WriteRetentionStore>>();
	
	// One should find the same WriteRetentionStore given a write retention time and a target store
	/**
	 * Returns a {@link WriteRetentionStore} with s as {@link DelegatingStore#getActualStore() delegate} ;
	 * in case s is already a {@link WriteRetentionStore} with a different {@link #getWriteRetentionMs()},
	 * returns another {@link WriteRetentionStore} with same {@link DelegatingStore#getActualStore() delegate}
	 * as s.
	 * @param writeRetentionMs time during which updates are retended to delegate store
	 * @param s the actual store, or a {@link WriteRetentionStore} with delegating to the actual store
	 * @throws IllegalArgumentException if s is a delegation chain that already contains a {@link WriteRetentionStore}
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
		
		Collection<WriteRetentionStore> res;
		// Return candidate if not exists
		WriteRetentionStore ret = new WriteRetentionStore(writeRetentionMs, s);
		int h = ret.hashCode();
		
		// Getting existing stores for this hash
		synchronized(knownStores) {
			res = knownStores.get(h);
			
			// No known store for this hash
			if (res == null) {
				res = new LinkedList<WriteRetentionStore>();
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

	/**
	 * The approximate number of pending write requests.
	 */
	public static int getPendingRequests() {
		return writeQueue.size() + requestsBeingSending.get();
	}

	/**
	 * Thrown when attempting to merge to a dead request, i.e. removed from known requests
	 */
	private static class RequestIsOutException extends Exception {
		private static final long serialVersionUID = 4904072123077338091L;
	}

	/**
	 * Hash for finding a request according to the element to update
	 */
	private static class RowInTable implements Comparable<RowInTable> {
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

		@Override
		public int compareTo(RowInTable o) {
			if (o == null)
				return 1;
			if (this == o)
				return 0;
			return this.h - o.h;
		}
	}

	/**
	 * Summarize requests for a given row in a given table. Thread-safely merges
	 * with another request.
	 */
	private class StoreRequest implements Delayed {

		/**
		 * A merged datum is stored with a transaction index
		 */
		private final AtomicLong transactionDistributor = new AtomicLong(
				Long.MIN_VALUE);
		
		/**
		 * Last transaction that was sent, be it completed or not ; negative if not sent yet
		 */
		private volatile Long lastSentTransaction = null;

		/**
		 * Whether this request was sent and this request object must not be used anymore
		 */
		private volatile boolean dead = false;

		/**
		 * A lock to ensure this request is not updated while it's sent
		 */
		private final ReentrantReadWriteLock sendLock = new ReentrantReadWriteLock();

		/**
		 * The epoch date in nanoseconds at which this request should be sent to
		 * the data store ; -1 in case request is not planned
		 */
		private final AtomicLong outDateNanos = new AtomicLong(-1);
		
		/**
		 * Merged meta information
		 */
		private final AtomicReference<MetaInformation> meta = new AtomicReference<MetaInformation>();
		
		/**
		 * The updated row (table and id)
		 */
		private final RowInTable row;
		
		/**
		 * A map storing request data according to transaction number. Column
		 * can be a {@link byte[]}, a {@link WriteRetentionStore#DELETED_VALUE
		 * delete marker}, or a {@link Number}. If {@link byte[]} or
		 * {@link WriteRetentionStore#DELETED_VALUE delete marker}, only latest
		 * value is valid; if {@link Number}, datum corresponds to an increment
		 * and all values for all transactions should be summed to get the actual
		 * increment.
		 */
		@Transient private volatile ConcurrentMap<
			String /* family */,
			ConcurrentMap<
				String /* column */,
				ConcurrentNavigableMap<
					Long /* transaction id */,
					Object /* byte[] || Long (increment) || DELETED_VALUE */
		>>> elements = new ConcurrentHashMap<String, ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>>>();

		/**
		 * When this element was last deleted.
		 */
		@Transient private volatile ConcurrentSkipListSet<Long> deletions = new ConcurrentSkipListSet<Long>();

		private StoreRequest(RowInTable row) {
			super();
			this.row = row;
		}

		/**
		 * Starts an update ; {@link #doneUpdate()} must absolutely be
		 * eventually called.
		 * 
		 * @return transaction number
		 * @throws RequestIsOutException
		 *             in case this request is {@link #dead}
		 */
		private long startUpdate() throws RequestIsOutException {
			if (this.dead) {
				throw new RequestIsOutException();
			}
			
			this.sendLock.readLock().lock();

			if (this.dead) {
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
		 *             in case this request is {@link #dead}
		 */
		public void delete(MetaInformation meta) throws RequestIsOutException {
			long transaction = this.startUpdate();
			try {
				this.addMeta(meta);
				
				this.deletions.add(transaction);

				// Cleaning up memory from all merged data before this transaction
				this.deletions.headSet(transaction, false).clear();
				for (Entry<String, ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>>> fam : this.elements.entrySet()) {
					for (Entry<String, ConcurrentNavigableMap<Long, Object>> col : fam.getValue().entrySet()) {
						col.getValue().headMap(transaction, false).clear();
					}
				}
			} finally {
				this.doneUpdate();
			}
		}

		/**
		 * Thread-safe merge with new data.
		 * 
		 * @throws RequestIsOutException
		 *             in case this request is {@link #dead}
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
							
							// Cleaning up memory from overridden values
							columnData.headMap(transactionId, false).clear();
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
							
							// NOT cleaning up memory as actual increment is the sum of all transactions
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
							
							// Cleaning up memory from overridden values
							columnData.headMap(transactionId, false).clear();
						}
					}
				}
			} finally {
				this.doneUpdate();
			}
		}

		/**
		 * Merging meta information
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
		
		@Override
		public int compareTo(Delayed d) {
			if (this == d)
				return 0;
			StoreRequest o = (StoreRequest) d;
			long thisOut = this.outDateNanos.get(), oOut = o.outDateNanos.get();
			if (thisOut == oOut) {
				return this.row.compareTo(o.row);
			} else if (thisOut < oOut)
				return -1;
			else // if (thisOut > oOut)
				return 1;
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return unit.convert(this.outDateNanos.get() - System.nanoTime(),
					TimeUnit.NANOSECONDS);
		}

		/**
		 * Sending this request. Waits for current updates to be done.
		 * 
		 * @param sender
		 *            the executor for sending the request
		 * @param counter
		 *            a counter to be decremented when the request is sent
		 * @param flushing
		 *            whether this send is a normal operation of a flush operation
		 */
		public void send(ExecutorService sender,
				final AtomicInteger counter, boolean flushing) {
			// Can be sent by the eviction thread after a flush
			if (this.dead) {
				// No new transaction is ignored
				assert transactionDistributor.get() <= (this.lastSentTransaction == null ? Long.MIN_VALUE : this.lastSentTransaction);
				return;
			}
			// Not sending this request before delay is expired unless we flush
			assert flushing || this.outDateNanos.get() <= System.nanoTime();
			long lastTransactionTmp;
			Long lastSentTransaction;
			// A copy of the elements, deletes and meta to be sent now
			ConcurrentMap<String, ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>>> elements;
			ConcurrentSkipListSet<Long> deletions;
			MetaInformation metaTmp;
			this.sendLock.writeLock().lock();
			try {
				// This code cannot be executed concurrently with an update
				
				lastTransactionTmp = transactionDistributor.get();
				// Is it really necessary to send those elements ?
				// Might not be the case if the element was flushed
				if (this.lastSentTransaction != null && lastTransactionTmp <= this.lastSentTransaction) {
					return;
				}
				lastSentTransaction = this.lastSentTransaction;
				this.lastSentTransaction = lastTransactionTmp;
				
				// Preparing this request to be used while current status is being sent by resetting all
				elements = this.elements;
				this.elements = new ConcurrentHashMap<String, ConcurrentMap<String,ConcurrentNavigableMap<Long,Object>>>();
				deletions = this.deletions;
				this.deletions = new ConcurrentSkipListSet<Long>();
				metaTmp = this.meta.getAndSet(null);
				// As from this line, there MUST be a send request
			} finally {
				this.sendLock.writeLock().unlock();
			}
			final MetaInformation meta = metaTmp;
			final long lastTransaction = lastTransactionTmp;
			
			// This request should be the one for this row
			assert this == writesByRows.get(this.row);

			// The action to be ran for sending this request
			Runnable action;
			counter.incrementAndGet();
			try {

				this.outDateNanos.set(-1);
				
				// As from this line, we are not considering transactions later than lastTransaction
				// If a new transaction happens, request will be planned again once sent
				
				Long lastDeletion = deletions.isEmpty() ? null : deletions.last();
				if (lastDeletion != null && lastSentTransaction != null && lastDeletion < lastSentTransaction) {
					assert false;
					lastDeletion = null;
				}
				// Last no-delete update in order to know whether this element should be deleted or not
				long lastStore = Long.MIN_VALUE;
	
				// Grabbing changed values.
				final ColumnFamilyData changes = new DefaultColumnFamilyData();
				final Map<String, Set<String>> removed = new TreeMap<String, Set<String>>();
				final Map<String, Map<String, Number>> increments = new TreeMap<String, Map<String, Number>>();
				for (Entry<String, ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>>> famData : elements
						.entrySet()) {
					for (Entry<String, ConcurrentNavigableMap<Long, Object>> colData : famData
							.getValue().entrySet()) {
						Entry<Long, Object> lastEntry = colData.getValue()
								.lastEntry();
						// There can be no entry in case of a row delete after a column update
						if (lastEntry == null)
							continue;
						Long transaction = lastEntry.getKey();
						if (lastSentTransaction != null && transaction < lastSentTransaction) {
							assert false;
							continue;
						}
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
									try {
										sum += ((Number) val).longValue();
									} catch (ClassCastException x) {
										// Just in case datum was updated (not incremented) meanwhile
										// This is robustness code as it's not likely to happen
										if (val == DELETED_VALUE)
											sum = 0;
										else // if (val instanceof byte[]) 
											sum = ConversionTools.convert(Long.class, (byte[])val).longValue();
									}
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
	
				assert lastDeletion == null || lastTransaction >= lastDeletion;
				assert lastTransaction >= lastStore;
	
				// Sending using the executor
				// Checking whether it's a store or a delete
				if (lastDeletion == null || lastDeletion < lastStore) {
					
					// A delete resets all columns ;
					// cannot simulate that just using a store
					final boolean shouldDelete = lastDeletion != null;
					
					action = new Runnable() {
	
						@Override
						public void run() {
							try {
								// Deleting all cells if necessary
								if (shouldDelete)
									getActualStore().delete(meta, row.table, row.id);
								
								getActualStore().storeChanges(meta, row.table, row.id,
										changes, removed, increments);
								
							} catch (RuntimeException x) {
								x.printStackTrace();
								throw x;
							} finally {
								counter.decrementAndGet();
								requestSent(lastTransaction);
							}
						}
					};
				} else {
					action = new Runnable() {
	
						@Override
						public void run() {
							try {
								getActualStore().delete(meta, row.table, row.id);
							} catch (RuntimeException x) {
								x.printStackTrace();
								throw x;
							} finally {
								counter.decrementAndGet();
								requestSent(lastTransaction);
							}
						}
					};
				}
			
			} catch (Throwable r) {
				counter.decrementAndGet();
				throw r instanceof RuntimeException ? (RuntimeException)r : new RuntimeException(r);
			}
			
			if (sender == null)
				action.run();
			else
				sender.submit(action);
		}
		
		/**
		 * Called once request was actually sent
		 * @param lastTransactionBeforeSending 
		 */
		private void requestSent(long lastTransactionBeforeSending) {
			assert this.lastSentTransaction >= lastTransactionBeforeSending;
			this.sendLock.writeLock().lock();
			try {
				// It's last sent transaction that should check whether this element should be back in the queue
				if (lastTransactionBeforeSending < this.lastSentTransaction)
					return;
				// No update can happen when executing this section
				if (this.transactionDistributor.get() > this.lastSentTransaction) {
					// This request is THE only request for its row
					assert this == writesByRows.get(this.row);
					// A transaction happened while sending ; re-planning
					this.plan();
				} else {
					// No transaction happened ; killing this object and releasing memory
					this.dead = true;
					StoreRequest s = writesByRows.remove(this.row);
					// This request was THE only request for its row
					assert this == s;
				}
				
			} finally {
				this.sendLock.writeLock().unlock();
			}
		}

		/**
		 * Computing next update and registering in the delay queue
		 */
		public void plan() {
			// Cannot plan if this request is out
			assert !this.dead;
			// Cannot plan an already registered request
			long nextExecutionDate = TimeUnit.NANOSECONDS.convert(WriteRetentionStore.this.writeRetentionMs,
				TimeUnit.MILLISECONDS) + System.nanoTime();
			if (this.outDateNanos.compareAndSet(-1, nextExecutionDate)) {
				writeQueue.put(this);
			} else {
				assert writeQueue.contains(this);
			}
		}

	}

	/**
	 * Code for the thread responsible for reading {@link WriteRetentionStore#writeQueue the queue} and
	 * {@link StoreRequest#send(ExecutorService, AtomicInteger) sending requests}. Only one thread should 
	 * live. A shutdown hook sends all pending requests.
	 */
	private static class EvictionThread extends Thread {
		private final ExecutorService sender = Executors.newCachedThreadPool(new ThreadFactory() {
			
			@Override
			public Thread newThread(Runnable r) {
				Thread ret = new Thread(r, "n-orm write cache sender");
				ret.setDaemon(false);
				return ret;
			}
		});

		private volatile boolean alreadyStarted = false;
		private volatile boolean running = true;
		
		private EvictionThread() {
			super("n-orm write cache eviction thread");
			this.setDaemon(false);
			this.setPriority(Thread.MAX_PRIORITY);
		}

		@Override
		public synchronized void start() {
			if (this.alreadyStarted)
				return;
			this.alreadyStarted = true;
			super.start();
			// Registering last chance to evict all the list of elements in the
			// store
			Runtime.getRuntime().addShutdownHook(new Thread("n-orm write cache shutdown handler") {

				@Override
				public void run() {
					try {
						// Prevents new requests to be recorded
						shutdown = true;
						// Stops the queue dequeue
						running = false;
						// Stop waiting for the queue
						EvictionThread.this.interrupt();

						// Sending pending requests using one single thread
						ExecutorService sender = Executors
								.newSingleThreadExecutor(new ThreadFactory() {
									
									@Override
									public Thread newThread(Runnable r) {
										Thread ret = new Thread(r, "n-orm write cache flush before shutdown");
										ret.setPriority(MAX_PRIORITY);
										ret.setDaemon(false);
										return ret;
									}
								});
						boolean removed;
						do {
							Iterator<StoreRequest> it = writeQueue.iterator();
							removed = it.hasNext();
							while (it.hasNext()) {
								it.next()
										.send(	sender,
												requestsBeingSending,
												true);
								it.remove();
							}
						} while (removed);

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
					// Requests in preparation of sending are marked twice so that a "0" is
					// a bit less likely to be a false 0
					requestsBeingSending.incrementAndGet();
					r.send(this.sender, requestsBeingSending, false);
					requestsBeingSending.decrementAndGet();
				} catch (InterruptedException e) {
				} catch (Throwable e) {
					System.err.println("Problem while sending request out of write cache");
					e.printStackTrace();
				}
			}
		}

	}

	private abstract static class Operation {
		public abstract void run(StoreRequest req) throws RequestIsOutException;
	}
	
	static {
		if (DELETED_VALUE == new byte[0])
			throw new Error("JVM problem : " + WriteRetentionStore.class.getSimpleName() + " requires that new byte[0] != new byte[0]");
		evictionThread = new EvictionThread();
		evictionThread.start();
	}
	
	// Index for requests according to the row
	@Transient
	private final ConcurrentMap<RowInTable, StoreRequest> writesByRows = new ConcurrentHashMap<RowInTable, StoreRequest>();
	
	//Whether this store was already started
	private AtomicBoolean started = new AtomicBoolean(false);

	// Time before request is sent
	private long writeRetentionMs;
	
	private WriteRetentionStore(long writeRetentionMs, Store s) {
		super(s);
		this.writeRetentionMs = writeRetentionMs;
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
			super.start();
		}
	}
	
	/**
	 * Sends immediately planned requests for the given element.
	 * @param table the table of the element
	 * @param identifier the identifier of the element
	 * @return whether some requests were found regarding this element
	 */
	public boolean flush(String table, String identifier) {
		RowInTable row = new RowInTable(table, identifier);
		StoreRequest req = this.writesByRows.get(row);
		if (req != null) {
			req.send(null, requestsBeingSending, true);
			return true;
		} else
			return false;
	}

	@Override
	public void delete(final MetaInformation meta, String table, String id)
			throws DatabaseNotReachedException {
		if (shutdown) {
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
		if (shutdown) {
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
		while(true) {
			StoreRequest req = new StoreRequest(element);
			StoreRequest tmp = writesByRows.putIfAbsent(element, req);
			if (tmp == null) {
				// req was added ; should also be put in the delay queue
				req.plan();
				//System.out.println("Request planned for " + table + ':' + id + " on " + System.currentTimeMillis() + " by " + req);
			} else {
				// Another thread added request for this element before us
				req = tmp;
			}
			try {
				r.run(req);
				// Request is planned and merged ; leaving the infinite loop
				break;
			} catch (RequestIsOutException x) {
				// We've tried to update a request that went out meanwhile
			}
			// retrying eventually
			Thread.yield();
		}
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
