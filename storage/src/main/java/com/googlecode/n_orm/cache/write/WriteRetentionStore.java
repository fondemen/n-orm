package com.googlecode.n_orm.cache.write;

import java.util.Collection;
import java.util.Date;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PersistingMixin;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.Transient;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.DelegatingStore;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Store;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.utils.LongAdder;

/**
 * A {@link DelegatingStore} that retains all writes
 * ({@link #storeChanges(MetaInformation, String, String, ColumnFamilyData, Map, Map)}
 * and {@link #delete(MetaInformation, String, String)}, that is {@link PersistingElement#store()} and
 * {@link PersistingElement#delete()}) during a certain amount of time. This kind of store is obviously
 * thread safe, and interesting in case rows are often updated by dramatically lowering the number of
 * write requests on the {@link #getActualStore() actual store}.
 * <p>Writes are actually issued to the {@link #getActualStore() actual store} after a
 * {@link #getWriteRetentionMs() given time}. If other writes for the same persisting element occur during
 * this period, they are merged with retained write for this persisting element and eventually sent.
 * As an example, if a persisting element is stored at time t1, changed and stored again at time t2
 * such that t2-t1 is less than the {@link #getWriteRetentionMs() retention time}, only one store request
 * is issued to the {@link #getActualStore() actual store}, after t1+{@link #getWriteRetentionMs() retention time}.</p>
 * <p>Reads (as {@link #get(MetaInformation, String, String, Set)}, {@link PersistingElement#activate(Object[])})
 * do not explore data "retended" here. As a example, activating, changing, storing, and then
 * activating again a persisting element using this kind of store can re-activate it as it was before the change.
 * Same remark holds for testing an element of existence, counting element, or getting a list of elements
 * that match criteria.</p>
 * <p>This store can be deactivated ; in the latter case, it merely acts as a delegating store with no delay.
 * To en/de-activate, use {@link #setEnabledByDefault(boolean)}. A thread can anyway be authorized to activate for
 * itself (and only itself) any de-activated write retention store if calling before
 * {@link #setEnabledForCurrentThread(boolean)}.</p>
 * <p>This store can be supervised using {@link #getPendingRequests()}. To grab more metrics (such as the number of
 * requests {{@link #getRequestsIn()} in}, {@link #getRequestsOut() out},
 * {@link #getAverageLatencyMs() time between a request should be sent and is sent}...), you need to enable
 * {@link #setCapureHitRatio(boolean)} to true, which is not the case by default as it introduces some overhead.
 * It is possible to enable and then disable addtional capture on a regular basis to get metrics samples.
 * In this latter case, older metrics can be {@link #resetCapureHitRatioMetrics() reseted}.</p>
 * <p>This store can be activated using {@link Persisting#writeRetentionMs() the @Persisting annotation} on a class
 * or by setting the {@link StoreSelector#STORE_WRITE_RETENTION with-write-retention} property on the
 * storage.properties file.</p>
 */
public class WriteRetentionStore extends DelegatingStore {
	public static final Logger logger = Logger.getLogger(DelegatingStore.class.getName()); 

	private static final byte[] DELETED_VALUE = new byte[0];
	private static final byte[] NULL_VALUE = new byte[0];
	
	/**
	 * Event queue
	 */
	private static final DelayQueue<StoreRequest> writeQueue = new DelayQueue<StoreRequest>();
	
	/**
	 * Whether write retention should be enabled for a given thread
	 */
	private static ThreadLocal<Boolean> enabled = new ThreadLocal<Boolean>();
	
	/**
	 * Number of requests currently being sending
	 */
	private static final AtomicLong requestsBeingSending = new AtomicLong();
	
	/**
	 * Whether or not hit ratio should be captured
	 */
	private static boolean captureHitRatio = false;

	/**
	 * Number of requests asked
	 */
	private static final LongAdder requestsIn = new LongAdder();
	
	/**
	 * Number of requests sent
	 */
	private static final LongAdder requestsOut = new LongAdder();
	
	/**
	 * Cumulative latencies
	 */
	private static final LongAdder requestsCumulativeLatency = new LongAdder();
	
	/**
	 * Number of latencies accumulated into {@link #requestsCumulativeLatency}
	 */
	private static final LongAdder requestsLatencySamples = new LongAdder();
	
	/**
	 * Whether we are being stopping (JVM shutdown)
	 */
	private static volatile boolean shutdown = false;

	/**
	 * Thread responsible for dequeue
	 */
	private static final EvictionThread evictionThread;
	
	/**
	 * Maximum global number of threads used for sending requests to store
	 */
	private static volatile int MAX_SENDER_THREADS = 40;
	
	/**
	 * Known stores
	 */
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
	 * Whether write retention should be enabled for this thread even for
	 * {@link #setEnabledByDefault(boolean) de-activated} write-retention stores.
	 */
	public static boolean isEnabledForCurrentThread() {
		Boolean ret = enabled.get();
		return ret != null && ret;
	}

	/**
	 * Whether write retention should be enabled for this thread even for
	 * {@link #setEnabledByDefault(boolean) de-activated} write-retention stores.
	 */
	public static void setEnabledForCurrentThread(boolean enabled) {
		WriteRetentionStore.enabled.set(enabled);
	}

	/**
	 * Maximum global number of threads used for sending requests to store ; default is 40.
	 */
	public static int getMaxSenderThreads() {
		return MAX_SENDER_THREADS;
	}

	/**
	 * Maximum global number of threads used for sending requests to store
	 */
	public static void setMaxSenderThreads(int maxSenderThreads) {
		MAX_SENDER_THREADS = maxSenderThreads;
	}


	/**
	 * The approximate number of pending write requests.
	 */
	public static int getPendingRequests() {
		return writeQueue.size() + requestsBeingSending.intValue();
	}
	
	/**
	 * Whether hit ratio metrics should be captured
	 */
	public static boolean isCapureHitRatio() {
		return WriteRetentionStore.captureHitRatio;
	}
	
	/**
	 * Whether hit ratio metrics should be captured
	 */
	public static void setCapureHitRatio(boolean captureHitRatio) {
		WriteRetentionStore.captureHitRatio = captureHitRatio;
	}
	
	/**
	 * Resets any hit-ratio-related metrics {@link #getHitRatio()}, {@link #getRequestsIn()}, {@link #getRequestsOut()}, and {@link #getAverageLatencyMs()}
	 */
	public static void resetCapureHitRatioMetrics() {
		requestsLatencySamples.reset();
		requestsCumulativeLatency.reset();
		requestsIn.reset();
		requestsIn.add(getPendingRequests());
		requestsOut.reset();
	}

	/**
	 * Number of requests asked ; {@link #isCapureHitRatio()} should be on to capture this metric
	 */
	public static long getRequestsIn() {
		return requestsIn.longValue();
	}

	/**
	 * Number of requests sent ; {@link #isCapureHitRatio()} should be on to capture this metric
	 */
	public static long getRequestsOut() {
		return requestsOut.longValue();
	}
	
	/**
	 * The hit ratio (requests asked / requests (to be)sent ; {@link #isCapureHitRatio()} should be on to capture this metric
	 */
	public static double getHitRatio() {
		if (getRequestsIn() == 0)
			return Double.NaN;
		long in = getRequestsIn();
		return (double)(in - getRequestsOut() - getPendingRequests()) / (double)(in);
	}
	
	/**
	 * The cumulative latency (ms) between time at which a request should have been sent and the time it is actually sent
	 */
	public static long getCumulativeLatencyMs() {
		return requestsCumulativeLatency.longValue();
	}
	
	/**
	 * Number of latencies accumulated by {@link #getCumulativeLatencyMs()}
	 */
	public static long getLatencySamples() {
		return requestsLatencySamples.longValue();
	}
	
	/**
	 * The average latency (ms) between time at which a request should have been sent and the time it is actually sent
	 */
	public static long getAverageLatencyMs() {
		if (getLatencySamples() == 0)
			return 0;
		return getCumulativeLatencyMs() / getLatencySamples();
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
			int ret = this.h - o.h;
			if (ret != 0)
				return ret;
			return this.table.compareTo(o.table) + this.id.compareTo(o.id);
		}
		
		@Override
		public String toString() {
			return "element " + PersistingMixin.getInstance().identifierToString(this.id) + " in table " + this.table;
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
		 * Whether this element is being sending
		 */
		private volatile boolean sending = false;

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
		private final AtomicLong outDateMs = new AtomicLong(-1);
		
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
		
		/**
		 * The time at which transaction was planned to be released
		 */
		@Transient private final ConcurrentNavigableMap<Long /* transaction */, Long /* out date */> outDates = new ConcurrentSkipListMap<Long, Long>();

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

			long ret = this.transactionDistributor.incrementAndGet();
			
			assert this.lastSentTransaction == null || ret > this.lastSentTransaction;

			if (captureHitRatio) {
				long outDate = this.outDateMs.get();
				if (outDate != -1) {
					Entry<Long, Long> old = outDates.floorEntry(ret);
					assert old == null || old.getValue() != -1;
					if (old == null || old.getValue() != outDate)
						outDates.put(ret, outDate);
				}
			}
			
			return ret;
		}

		/**
		 * Declares an update end.
		 * 
		 * @throws RequestIsOutException
		 */
		private void doneUpdate(long transaction) throws RequestIsOutException {
			assert this.lastSentTransaction == null || transaction > this.lastSentTransaction;
			
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
				this.doneUpdate(transaction);
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
							byte[] chval = colChange.getValue();
							Object old = columnData.put(transactionId,
									chval == null ? NULL_VALUE : chval);
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
				this.doneUpdate(transactionId);
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
			long thisOut = this.outDateMs.get(), oOut = o.outDateMs.get();
			if (thisOut == oOut) {
				return this.row.compareTo(o.row);
			} else if (thisOut < oOut)
				return -1;
			else // if (thisOut > oOut)
				return 1;
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return unit.convert(this.outDateMs.get() - System.currentTimeMillis(),
					TimeUnit.MILLISECONDS);
		}
		
		/**
		 * Tells this request that is is out of the planning queue
		 */
		private void dequeued() {
			
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
				final AtomicLong counter, final boolean flushing) {
			// Not sending this request before delay is expired unless we flush
			assert flushing || this.outDateMs.get() <= System.currentTimeMillis();
			long lastTransactionTmp;
			Long lastSentTransaction;
			long outDateTmp;
			// A copy of the elements, deletes and meta to be sent now
			ConcurrentMap<String, ConcurrentMap<String, ConcurrentNavigableMap<Long, Object>>> elements;
			ConcurrentSkipListSet<Long> deletions;
			MetaInformation metaTmp;
			this.sendLock.writeLock().lock();
			try {
				// This code cannot be executed concurrently with an update or another send start/stop
				if (!flushing)
					this.outDateMs.set(-1);

				lastTransactionTmp = transactionDistributor.get();
				boolean shouldLeave = false;
				if (this.sending) {
					// Already sending ? (e.g. long flush just before) => giving up
					shouldLeave = true;
				
				} else {
					// Newer transaction was sent, giving up
					if (this.lastSentTransaction != null && lastTransactionTmp <= this.lastSentTransaction) {
						shouldLeave = true;
					}
				}
				
				if (shouldLeave) {
					if (! flushing) {
						// Replanning
						try {
							this.plan();
						} catch (RequestIsOutException x) {
							assert this.lastSentTransaction == null ? this.transactionDistributor.get() == Long.MIN_VALUE : this.lastSentTransaction == this.transactionDistributor.get();
						}
					}
					return;
				}
				
				// As from this line, there MUST be a send request
				this.sending = true;
				outDateTmp = this.outDateMs.get();
				
				lastSentTransaction = this.lastSentTransaction;
				this.lastSentTransaction = lastTransactionTmp;

				// This request should be the one for this row
				assert this == writesByRows.get(this.row);
				// Preparing this request to be used while current status is being sent by resetting all
				elements = this.elements;
				this.elements = new ConcurrentHashMap<String, ConcurrentMap<String,ConcurrentNavigableMap<Long,Object>>>();
				deletions = this.deletions;
				this.deletions = new ConcurrentSkipListSet<Long>();
				metaTmp = this.meta.getAndSet(null);
				counter.incrementAndGet();
				
			} finally {
				this.sendLock.writeLock().unlock();
			}
			final MetaInformation meta = metaTmp;
			final long lastTransaction = lastTransactionTmp;
			final long outDate = outDateTmp;

			// The action to be ran for sending this request
			Runnable action;				
			try {
				
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
									chgCols.put(colData.getKey(), latest == null ? null : (byte[]) latest);
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
				
				// Capturing planned out date in case of hit ratio computation
				boolean captureHitRatio = WriteRetentionStore.captureHitRatio;
				if (captureHitRatio)
					this.outDates.headMap(lastTransaction, true).clear();
	
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
								try {
									if (!flushing && outDate != -1) {
										requestsLatencySamples.increment();
										long delay = System.currentTimeMillis()-outDate;
										assert delay > 0;
										requestsCumulativeLatency.add(delay);
									}
								} finally {
								
									// Deleting all cells if necessary
									if (shouldDelete)
										getActualStore().delete(meta, row.table, row.id);
									
									getActualStore().storeChanges(meta, row.table, row.id,
											changes, removed, increments);
								}
								
							} catch (RuntimeException x) {
								logger.log(Level.WARNING, "Catched problem while updating " + StoreRequest.this + " ; some data might have been lost: " + x.getMessage(), x);
								throw x;
							} finally {
								counter.decrementAndGet();
								requestSent(flushing, lastTransaction);
							}
						}
					};
				} else {
					action = new Runnable() {
	
						@Override
						public void run() {
							try {
								try {
									if (!flushing && outDate != -1) {
										requestsLatencySamples.increment();
										long delay = System.currentTimeMillis()-outDate;
										assert delay > 0;
										requestsCumulativeLatency.add(delay);
									}
								} finally {
								
									getActualStore().delete(meta, row.table, row.id);
								}
							} catch (RuntimeException x) {
								logger.log(Level.WARNING, "Catched problem while deleting " + StoreRequest.this + " ; some data might have been be lost: " + x.getMessage(), x);
								throw x;
							} finally {
								counter.decrementAndGet();
								requestSent(flushing, lastTransaction);
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
		 * @param lastTransactionBeforeSending the sent transaction
		 * @param expectedOutDate time at which request was planned ; null in case of a flush
		 */
		private void requestSent(boolean afterFlush, long lastTransactionBeforeSending) {
			if (captureHitRatio)
				requestsOut.increment();
			assert this.lastSentTransaction == lastTransactionBeforeSending;
			assert this.sending;
			this.sendLock.writeLock().lock();
			try {
				this.sending = false;

				// No update can happen when executing this section
				if (this.transactionDistributor.get() > this.lastSentTransaction) {
					// This request is THE only request for its row
					assert this == writesByRows.get(this.row);
					
					if (!afterFlush) {
						// A transaction happened while sending ; re-planning
						try {
							this.plan();
							logger.fine(this.toString() + " sent on " + new Date(System.currentTimeMillis()) + " replanned for " + new Date(this.outDateMs.get()));
						} catch (RequestIsOutException x) {
							// We are the only process authorized to try to plan at this point
							// (indeed, this store request is still indexed by writesByRows)
							assert false;
						}
					}
				} else {
					// No transaction happened ; killing this object and releasing memory
					this.close();
				}
				
			} finally {
				this.sendLock.writeLock().unlock();
			}
		}
		
		/**
		 * Closing request definitively ; MUST be invoked within a write lock
		 */
		private void close() {
			assert this.sendLock.writeLock().isHeldByCurrentThread();
			this.dead = true;
			StoreRequest s = writesByRows.remove(this.row);
			// This request was THE only request for its row
			assert this == s;
			logger.fine(this.toString() + " sent on " + new Date(System.currentTimeMillis()) + " and not replanned");
		}

		/**
		 * Computing next update and registering in the delay queue
		 */
		private void plan() throws RequestIsOutException {
			// Cannot plan if this request is out of indexed requests
			if (this.dead)
				throw new RequestIsOutException();

			long nextExecutionDate = WriteRetentionStore.this.writeRetentionMs + System.currentTimeMillis();
			if (this.outDateMs.compareAndSet(-1, nextExecutionDate)) {
				writeQueue.put(this);
			} else {
				// Request should already be planned
				// Checking this only in case of assertion as it's costly
				assert writeQueue.contains(this);
			}
		}
		
		@Override
		public String toString() {
			return "write-cached "
					+ (this.dead ? "sent" : "")
					+ " request for "
					+ this.row
					+ (this.dead ? 
							"" :
							"planned for " +
								new Date(this.outDateMs.get()) +
								" (in " +
								(this.outDateMs.get()-System.currentTimeMillis()) +
								"ms)");
		}
	}

	/**
	 * Code for the thread responsible for reading {@link WriteRetentionStore#writeQueue the queue} and
	 * {@link StoreRequest#send(ExecutorService, LongAdder) sending requests}. Only one thread should 
	 * live. A shutdown hook sends all pending requests.
	 */
	private static class EvictionThread extends Thread {
		private final ExecutorService sender = Executors.newFixedThreadPool(MAX_SENDER_THREADS, new ThreadFactory() {
			
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
						
						assert writeQueue.isEmpty();

					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}

			});
		}

		@Override
		public void run() {
			while (this.running) {
				StoreRequest r = null;
				try {
					r = writeQueue.take();
					// Requests in preparation of sending are marked twice so that a "0" is
					// a bit less likely to be a false 0
					requestsBeingSending.incrementAndGet();
					r.send(this.sender, requestsBeingSending, false);
					requestsBeingSending.decrementAndGet();
				} catch (InterruptedException e) {
				} catch (Throwable e) {
					if (r == null) {
						logger.log(Level.WARNING, "Problem waiting for request out of write cache: " + e.getMessage(), e);
					} else {
						logger.log(Level.SEVERE, "Problem while sending request out of write cache ; request " + r + " lost: " + e.getMessage(), e);
					}
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
	
	/**
	 * Index for requests according to the row
	 */
	@Transient
	private final ConcurrentMap<RowInTable, StoreRequest> writesByRows = new ConcurrentHashMap<RowInTable, StoreRequest>();
	
	/**
	 * Whether this store was already started
	 */
	private AtomicBoolean started = new AtomicBoolean(false);

	/**
	 * Time before request is sent
	 */
	private long writeRetentionMs;
	
	/**
	 * Whether write retention should be enabled by default
	 */
	private boolean enabledByDefault = true;
	
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

	/**
	 * Whether write retention is enabled by default for thread that did not call {@link #setEnabledForCurrentThread(boolean)}.
	 */
	public boolean isEnabledByDefault() {
		return this.enabledByDefault;
	}


	/**
	 * Whether write retention is enabled by default for thread that did not call {@link #setEnabledForCurrentThread(boolean)}.
	 */
	public void setEnabledByDefault(boolean enabled) {
		this.enabledByDefault = enabled;
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
		if (!isRetending()) {
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
		if (!isRetending()) {
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
	 * Whether this store is actually retending writes.
	 * It will return true if JVM is not in a shutdown process and if store is
	 * {@link #setEnabledByDefault(boolean) enabled} or thread is
	 * {@link #setEnabledForCurrentThread(boolean) enabled}.
	 */
	private boolean isRetending() {
		return !shutdown && (this.isEnabledByDefault() || isEnabledForCurrentThread());
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
		if (captureHitRatio)
			requestsIn.increment();
		RowInTable element = new RowInTable(table, id);
		while(true) {
			StoreRequest req = new StoreRequest(element);
			StoreRequest tmp = writesByRows.putIfAbsent(element, req);
			try {
				if (tmp == null) {
					// req was added ; should also be put in the delay queue
					req.plan();
					logger.fine("Request planned for " + table + ':' + id + " on " + System.currentTimeMillis() + " by " + req);
				} else {
					// Another thread added request for this element before us
					req = tmp;
				}
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
