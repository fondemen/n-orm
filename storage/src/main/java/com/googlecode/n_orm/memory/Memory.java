package com.googlecode.n_orm.memory;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.EmptyCloseableIterator;
import com.googlecode.n_orm.Transient;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.memory.EvictionQueue.EvictionThread;
import com.googlecode.n_orm.memory.Memory.Table.Row;
import com.googlecode.n_orm.memory.Memory.Table.Row.ColumnFamily;
import com.googlecode.n_orm.memory.Memory.Table.Row.ColumnFamily.ByteValue;
import com.googlecode.n_orm.memory.Memory.Table.Row.ColumnFamily.Value;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.SimpleStore;

/**
 * Thread-safe (to our best knowledge) reference implementation for a store based on {@link ConcurrentSkipListMap}s.
 * This store entirely resides into memory, and is only available for the current JVM.
 * This store does not supports mixing incrementing and absolute values on the same column qualifier.
 * In addition to others {@link SimpleStore}, this store is able to automatically evict a row in case
 * is used {@link #storeChanges(String, String, ColumnFamilyData, Map, Map, Long)} instead of
 * {@link #storeChanges(String, String, ColumnFamilyData, Map, Map)}. However, unlike other caches,
 * eviction happens in any case (almost) after the delay is expired, even if the row is accessed
 * (read or write) eventually.
 */
public class Memory implements SimpleStore {
	public static final Memory INSTANCE = new Memory();
	
	/**
	 * Utility method used internally to find a sub-map with inclusive nullable start and stop keys.
	 * @param map the map to be searched-in
	 * @param fromIncl the start key ; can be null to state the start of the map
	 * @param toIncl the last key ; can be null to state the end of the map
	 * @return a sub-map (can be map or a view on map)
	 */
	protected static <T> NavigableMap<String, T> subMap(NavigableMap<String, T> map, String fromIncl, String toIncl) {
		switch ((fromIncl == null ? 0 : 2) + (toIncl == null ? 0 : 1)) {
		case 0: //no filter
			return map;
		case 1: //toIncl only
			return map.headMap(toIncl, true);
		case 2: //fromIncl only
			return map.tailMap(fromIncl, true);
		case 3: //two keys
			return map.subMap(fromIncl, true, toIncl, true);
		default:
			assert false;
			return map;
		}
		
	}

	/**
	 * A map that creates necessary keys as soon as they are requested by {@link #get(String)}.
	 * Use {@link #getNoCreate(String)} to avoid creating a new element lazily.
	 * Keys are all {@link String}.
	 * @param <T> the kind of elements owned by the map.
	 */
	private abstract class LazyMap<T> {
		
		/**
		 * The actual map owning elements.
		 */
		@Transient //Only here to avoid AspectJ weaving
		protected ConcurrentMap<String, T> map;
		
		/**
		 * @param sorted whether this map needs to be sorted according to keys natural order
		 */
		protected LazyMap(boolean sorted) {
			this.map = sorted ?  new ConcurrentSkipListMap<String,T>() : new ConcurrentHashMap<String, T>();
		}
		
		/**
		 * The actual map casted to NavigableMap.
		 * Can throw an exception if this map was not declared as sorted.
		 */
		@SuppressWarnings("unchecked")
		protected NavigableMap<String, T> getNavigableMap() {
			return (NavigableMap<String, T>)this.map;
		}
		
		/**
		 * Called by {@link #get(String)} when no value is found
		 * @param key the key for the newly created element
		 * @return the created element that is not in this map yet
		 */
		protected abstract T newElement(String key);

		/**
		 * Whether this key is known by this map
		 * @param key the key to search (cannot be null)
		 */
		public boolean contains(String key) {
			if (key == null)
				throw new NullPointerException();
			return map.containsKey(key);
		}

		/**
		 * Returns the value to the element stored with a given key.
		 * In case this element does not exists, a new one is created using {@link #newElement(String)}, stored in the map and returned.
		 * @param key the non-null key of the (possibly new) element
		 * @return the (possibly newly created) element stored under key key
		 */
		public T get(String key) {
			if (key == null)
				throw new NullPointerException();
			T ret = this.getNoCreate(key);
			if (ret == null) {
				T newT = this.newElement(key);
				T oldT = this.map.putIfAbsent(key, newT);
				ret = oldT == null ? newT : oldT;
			}
			return ret;
		}
		
		/**
		 * Returns the value to the element stored with a given key.
		 * @param key the non-null key of the (not new) element
		 * In case this element does not exists, returns null.
		 */
		protected T getNoCreate(String key) {
			if (key == null)
				throw new NullPointerException();
			return map.get(key);
		}

		/**
		 * Puts an element in the map.
		 * @param key the non-null key of the element
		 * @param value must not be null; use {@link #remove(String)} to remove an element
		 */
		public T put(String key, T value) {
			if (key == null)
				throw new NullPointerException();
			if (value == null)
				throw new NullPointerException();
			return map.put(key, value);
		}


		/**
		 * Removes an element from the map.
		 * @param key the non-null key of the element
		 * @param value must not be null; use {@link #remove(String)} to remove an element
		 * @return the previous value for the map
		 */
		public  T remove(String key) {
			if (key == null)
				throw new NullPointerException();
			return map.remove(key);
		}


		/**
		 * Removes elements from the map according to their keys.
		 * @param key the non-null key of the element
		 * @param value must not be null; use {@link #remove(String)} to remove an element
		 */
		public void removeAll(Set<String> keys) {
			if (keys == null)
				throw new NullPointerException();
			map.keySet().removeAll(keys);
		}

		/**
		 * Vacuums this map.
		 */
		public void clear() {
			this.map.clear();
		}
	}

	/**
	 * A value to state that a {@link ByteValue} is deleted
	 */
	private static final byte[] DELETED_VALUE = new byte[0];
	
	/**
	 * A value to state that a {@link ByteValue} is null
	 */
	private static final byte[] NULL_VALUE = new byte[0];
	
	/**
	 * A value to state that a {@link Table.Row} never expires
	 */
	private static final long NEVER_EXPIRES = -1;
	
	private static final ExecutorService ColumnRemover = Executors.newSingleThreadExecutor();
	
	/**
	 * An map to store rows within tables.
	 * Rows are indexed according to their keys.
	 * Rows are sorted within a table according to their key value so that rage search can be fast (see {@link #toMap(String, String)}.
	 */
	public class Table extends LazyMap<Table.Row> {
		
		/**
		 * The name for this table
		 */
		public final String name;
		
		public Table(String name) {
			super(true);
			this.name = name;
		}

		/**
		 * Creates a new {@link Row} with the given key
		 */
		protected Row newElement(String key) {
			return new Row(key);
		}
		
		/**
		 * Creates an {@link Iterator} over the {@link Row}s owned by this table starting from row with the given qualifier.
		 * @param fromKeyIncl the qualifier of the first row ; if a row with this qualifier does not exist, takes the row with the lowest key greater than fromKeyIncl
		 * @see Memory#subMap(NavigableMap, String, String)
		 */
		public Iterator<Row> getRowIterator(String fromKeyIncl) {
			return subMap(this.getNavigableMap(), fromKeyIncl, null).values().iterator();
		}
		
		/**
		 * A row owning a set of Column families.
		 * Column families are created lazily as soon as they are requested by {@link #get(String)}.
		 * This element can be set an eviction date: (almost) at that date, the row is removed from the table.
		 * Any {@link Memory#storeChanges(String, String, ColumnFamilyData, Map, Map) update} is waited for
		 * completion before notifying eviction listeners.
		 * In case of a write on this row (i.e. on its families and columns), you should first call
		 * {@link #startUpdate(Long)} and then {@link #stopUpdate()} once completed.
		 */
		public class Row extends LazyMap<Row.ColumnFamily> implements com.googlecode.n_orm.storeapi.Row {
			/**
			 * The identifier for this row
			 */
			public final String key;
			
			/**
			 * The date (epoch) when this row should be evicted.
			 * Default value is -1 (never).
			 */
			public final AtomicLong evictionDate = new AtomicLong(NEVER_EXPIRES);
			
			/**
			 * A lock to set {@link #pendingWriteOperations} and {@link #waitingForEviction}
			 *  in case of a programmed eviction
			 */
			private ReentrantReadWriteLock writeOperationUpdateLock = new ReentrantReadWriteLock();
			/**
			 * Number of {@link Memory#storeChanges(String, String, ColumnFamilyData, Map, Map) updates}
			 * being performed in case of a programmed eviction
			 */
			private AtomicInteger pendingWriteOperations = new AtomicInteger();
			/**
			 * A lock to wait for updates completion when an eviction is to be done
			 */
			private CountDownLatch waitingForEviction;
			
			public Row(String key) {
				super(false);
				this.key = key;
			}

			/**
			 * Creates a column family according to its name.
			 */
			@Override
			protected ColumnFamily newElement(String name) {
				return new ColumnFamily(name);
			}

			/**
			 * The key for this row.
			 */
			@Override
			public String getKey() {
				return key;
			}

			@Override
			public ColumnFamilyData getValues() {
				ColumnFamilyData ret = new DefaultColumnFamilyData();
				for (Entry<String, ColumnFamily> element : map.entrySet()) {
					ret.put(element.getKey(), element.getValue().getValues(null, null));
				}
				return ret;
			}
			
			/**
			 * Sets the time to live for this row if not set already.
			 * Note that unlike other caches, this date is set only once, and eventual operations
			 * on this row do not change the planned eviction date.
			 * @param ttlMs the delay in milliseconds from now when this row should be evicted
			 */
			public void setEvictionDelay(long ttlMs) {
				if (ttlMs <= 0)
					throw new IllegalArgumentException("Attempting to set a TTL of " + ttlMs + "ms on row " + this.getKey() + " of table " + Table.this.name);
				if (this.evictionDate.get() == NEVER_EXPIRES)
					this.setEvictionDate(System.currentTimeMillis()+ttlMs);
			}
			
			/**
			 * Sets the eviction date for this row if and only if not set already.
			 * Setup the eviction queue in case it is the first to be set.
			 * @param evictionDate
			 */
			public void setEvictionDate(long evictionDate) {
				if (this.evictionDate.get() == NEVER_EXPIRES && this.evictionDate.compareAndSet(NEVER_EXPIRES, evictionDate)) {
					// It's the first time this row is set an eviction date...
					Memory.this.getEvictionQueue().put(evictionDate, this);
				}
			}
			
			/**
			 * Removes this row because of a passed eviction date.
			 * Though this method immediately removes the row from its table, this operation
			 * waits for any {@link Memory#storeChanges(String, String, ColumnFamilyData, Map, Map) update}
			 * completion before returning.
			 * No one is supposed to call this method but {@link EvictionThread}
			 */
			public void evict() throws InterruptedException {
				if (! (Thread.currentThread() instanceof EvictionQueue.EvictionThread))
					throw new IllegalAccessError("Only an eviction thread can evict a row");
				
				if (this.pendingWriteOperations == null)
					return; //Already evicted

				this.writeOperationUpdateLock.writeLock().lock();
				try {
					Table.this.map.remove(this.key, this);
					
					// Preparing wait on update operations completions if necessary
					int updates = this.pendingWriteOperations.get();
					if (updates > 0)
						waitingForEviction = new CountDownLatch(updates);
					this.pendingWriteOperations = null;
				} finally {
					this.writeOperationUpdateLock.writeLock().unlock();
				}
				
				// Waiting on update completions as some are performing
				if (this.waitingForEviction != null) {
					this.waitingForEviction.await(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
				}
			}

			/**
			 * Method to be called before any update to this row (i.e. it columns).
			 * In case this methode returns false, do not change this row at it has certainly been removed from the table.
			 * @param ttlMs the time-to-live in milliseconds expected for this row (see {@link #setEvictionDelay(long)}) ;
			 * 		null means no ttl is to be set
			 * @return false if this row should not be changed
			 */
			public boolean startUpdate(Long ttlMs) {
				if (ttlMs != null)
					this.setEvictionDelay(ttlMs);
				
				// We're not in eviction mode
				if (evictionDate.get() == NEVER_EXPIRES)
					return true;
				
				// There must be another thread running evict()
				if (this.pendingWriteOperations == null) {
					Thread.yield();
					return false;
				}
				
				this.writeOperationUpdateLock.readLock().lock();
				try {
					// There must be another thread running evict()
					if (this.pendingWriteOperations == null) {
						Thread.yield();
						return false;
					}
					// Registering our update intent
					this.pendingWriteOperations.incrementAndGet();
					return true;
				} finally {
					this.writeOperationUpdateLock.readLock().unlock();
				}
			}

			/**
			 * This operation should be called after {@link #startUpdate(Long)} and updates on this row.
			 * Not doing this after {@link #startUpdate(Long)} leads to dramatic deadlocks pretty much
			 * like forgetting an {@link Lock#unlock()}...
			 */
			public void stopUpdate() {
				// We're not in eviction mode
				if (evictionDate.get() == NEVER_EXPIRES)
					return;
				
				// No need to lock
				assert this.pendingWriteOperations != null;
				if (this.pendingWriteOperations != null) {
					// Registering our update is done
					this.pendingWriteOperations.decrementAndGet();
				} else if (this.waitingForEviction != null) {
					// It seems that the eviction thread was waiting for us...
					this.waitingForEviction.countDown();
				} else {
					assert false : "Neither update counter nor eviction countdown while signaling update end";
				}
			}

			/**
			 * A column family owning values.
			 * Values are sorted according to their qualifier so that rage search can be fast (see {@link #toMap(String, String)}.
			 */
			public class ColumnFamily extends LazyMap<ColumnFamily.Value<?>> {
				/**
				 * The name for this column family
				 */
				public final String name;
				
				public ColumnFamily(String name) {
					super(true);
					this.name = name;
				}

				/**
				 * Creates a byte value
				 */
				@Override
				protected Value<?> newElement(String qualifier) {
					return new ByteValue(qualifier);
				}

				/**
				 * Increments the value at the given key.
				 * @param qualifier the qualifier of the incremented value
				 * @param increment the non-null increment to be applied
				 */
				public void incr(String qualifier, Number increment) {
					assert increment.longValue() != 0 : "Received a 0 increment for table" + Table.this.name + ", row " + Row.this.key + ", family " + this.name + ", qualifier " + key; 
					IncrementingValue newVal = new IncrementingValue(qualifier, increment.longValue());
					Value<?> oldVal = this.map.putIfAbsent(qualifier, newVal);
					if (oldVal == null)
						return;
					else if (! (oldVal instanceof IncrementingValue))
						throw new IllegalStateException("Cannot increment a byte array value (value for " + qualifier + " in family " + this.name + " fro row " + Row.this.key + " in table " + Table.this.name + " is already set a non incrementing way");
					((IncrementingValue)oldVal).increment(increment.longValue());
				}
				
				/**
				 * The set of values in this column family mapped according to their qualifier.
				 * Qualifiers must be included between fromQualifierIncl and toQualifierIcl
				 */
				public Map<String, byte[]> getValues(String fromQualifierIncl, String toQualifierIcl) {
					Map<String, byte[]> ret = new TreeMap<String,byte[]>();
					for (Entry<String, Value<?>> element : subMap(this.getNavigableMap(), fromQualifierIncl, toQualifierIcl).entrySet()) {
						byte[] val = element.getValue().getBytes();
						if (val != DELETED_VALUE)
							ret.put(element.getKey(), val);
					}
					return ret;
				}
				
				@Override
				public final Value<?> remove(String key) {
					throw new IllegalStateException("Cannot remove a column without a transaction number");
				}

				@Override
				public final void removeAll(Set<String> keys) {
					throw new IllegalStateException("Cannot remove a column without a transaction number");
				}

				public abstract class Value<T> {
					/**
					 * The qualifier for this value
					 */
					public final String qualifier;
					
					/**
					 * Creates a value that cannot be incremented
					 * @param qualifier
					 * @param value
					 */
					public Value(String qualifier) {
						this.qualifier = qualifier;
					}
					
					/**
					 * The byte array representation for this value.
					 */
					public abstract byte[] getBytes();
				}
				
				public class ByteValue extends Value<byte[]> {
					
					/**
					 * The actual value for this value.
					 * Stored in a map according to transaction ids.
					 * Last entry holds the actual value.
					 */
					protected NavigableMap<Long /* last change */, byte[]> value = new ConcurrentSkipListMap<Long, byte[]>();
					
					protected ReentrantReadWriteLock deletionLock = new ReentrantReadWriteLock();
					
					/**
					 * The unsigned number of set operations.
					 */
					protected AtomicLong sets = new AtomicLong(Long.MIN_VALUE);

					public ByteValue(String qualifier) {
						super(qualifier);
						this.value.put(Long.MIN_VALUE, DELETED_VALUE);
					}
					
					public byte[] getValue() {
						Entry<Long, byte[]> lastEntry = this.value.lastEntry();
						if (lastEntry == null)
							return DELETED_VALUE;
						byte[] ret = lastEntry.getValue();
						return ret == null ? NULL_VALUE : ret;
					}
					
					public void setValue(byte[] value, long transaction) {
						if (value == null) value = NULL_VALUE;
						deletionLock.readLock().lock();
						try {
							if (this.value.isEmpty()) {
								// We've been deleted by a checkRemove ; retrying operation
								ByteValue newVal = (ByteValue)ColumnFamily.this.get(this.qualifier);
								assert newVal != this;
								newVal.setValue(value, transaction);
							} else {
								this.value.put(transaction, value);
							}
						} finally {
							deletionLock.readLock().unlock();
						}
						this.value.pollFirstEntry();
						
						if (this.getValue() == DELETED_VALUE) {
							// Deleting this column ASAP
							ColumnRemover.submit(new Runnable() {

								@Override
								public void run() {
									checkRemove();
								}
								
							});
						}
					}

					private void checkRemove() {
						// Should we remove this column ?
						if (this.getValue() == DELETED_VALUE) {
							// Value is a delete so it deserves to be lock-checked
							deletionLock.writeLock().lock();
							try {
								if (this.getValue() == DELETED_VALUE) {
									ColumnFamily.this.map.remove(this.qualifier);
									this.value.clear();
								}
							} finally {
								deletionLock.writeLock().unlock();
							}
						}
					}

					@Override
					public byte[] getBytes() {
						return this.getValue();
					}
				}
				
				public class IncrementingValue extends Value<Long> {
					
					/**
					 * The actual value for this value
					 */
					protected final AtomicLong value;

					public IncrementingValue(String qualifier, Long value) {
						super(qualifier);
						this.value = new AtomicLong(value);
					}
					
					public long getValue() {
						return this.value.get();
					}
					
					/**
					 * Atomically increments this value.
					 * @param increment can be negatve or positive
					 */
					public void increment(long increment) {
						this.value.getAndAdd(increment);
					}
					
					@Override
					public byte[] getBytes() {
						return ConversionTools.convert(this.value.get());
					}
				}
			}

		}
		
	}
	
	/**
	 * The set of tables in this store
	 */
	private LazyMap<Table> tables = new LazyMap<Table>(false) {

		/**
		 * Creates a table according to its name
		 */
		@Override
		protected Table newElement(String name) {
			return new Table(name);
		}
		
	};
	
	/**
	 * The next transaction number.
	 * Should increment over time.
	 */
	private AtomicLong nextTransactionId = new AtomicLong(Long.MIN_VALUE);
	
	/**
	 * The expiration queue in case this memory store was set an expiring row
	 */
	private AtomicReference<EvictionQueue> evictionQueue = new AtomicReference<EvictionQueue>();
	
	public Memory() {}

	@Override
	public void start() {
	}
	
	/**
	 * Returns the in-memory table.
	 * @param table the name of the expected table
	 * @param createIfNecessary whether to create the table if it does not exist
	 * @return the table ; can be null if createIfNecessary is set to false and the table does not exist
	 */
	public Table getTable(String table, boolean createIfNecessary) {
		return createIfNecessary ? this.tables.get(table) : this.tables.getNoCreate(table);
	}
	
	/**
	 * Returns the in-memory row (compatible with {@link com.googlecode.n_orm.storeapi.Row}) from a table.
	 * @param table the name of the table in which to find the row
	 * @param id the unique identifier for the row within the table
	 * @param createIfNecessary whether to create the row if it does not exist (including in case table does not exists)
	 * @return the row ; can be null if createIfNecessary is set to false and the row or the table does not exist
	 */
	public Row getRow(String table, String id, boolean createIfNecessary) {
		Table t = this.getTable(table, createIfNecessary);
		return createIfNecessary ? t.get(id) : (t == null ? null : t.getNoCreate(id));
	}

	/**
	 * Returns the in-memory column family for a given row from a table.
	 * @param table the name of the table in which to find the column family for the row
	 * @param id the unique identifier for the row within the table
	 * @param family the name of the column family to be found within the row
	 * @param createIfNecessary whether to create the row if it does not exist (including in case table or row does not exists)
	 * @return the row ; can be null if createIfNecessary is set to false and the column family or the row or the table does not exist
	 */
	public ColumnFamily getFamily(String table, String id, String family, boolean createIfNecessary) {
		Row r = this.getRow(table, id, createIfNecessary);
		return createIfNecessary ? r.get(family) : r == null ? null : r.getNoCreate(family);
	}

	@Override
	public boolean hasTable(String tableName) throws DatabaseNotReachedException {
		return this.tables.contains(tableName);
	}

	@Override
	public byte[] get(String table, String id, String family, String qualifer) {
		ColumnFamily fam = this.getFamily(table, id, family, false);
		Value<?> val = fam == null ? null : fam.getNoCreate(qualifer);
		byte[] ret = val == null ? null : val.getBytes();
		return ret == null || ret == DELETED_VALUE ? null : ret;
	}

	@Override
	public Map<String, byte[]> get(String table, String id, String family) {
		ColumnFamily fam = this.getFamily(table, id, family, false);
		return fam == null ? null : fam.getValues(null, null);
	}
	
	@Override
	public Map<String, byte[]> get(String table, String id, String family,
			Constraint c) throws DatabaseNotReachedException {
		ColumnFamily fam = this.getFamily(table, id, family, false);
		return fam == null ? null : fam.getValues(c == null ? null : c.getStartKey(), c == null ? null : c.getEndKey());
	}

	@Override
	public void storeChanges(String table, String id,
			ColumnFamilyData changed,
			Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> incremented) {
		this.storeChanges(table, id, changed, removed, incremented, null);
	}

	/**
	 * Stores changes as for {@link #storeChanges(String, String, ColumnFamilyData, Map, Map)},
	 * but programming an eviction as soon as a delay is passed.
	 * @param ttlMs the time to live of the element ; TTL won't be reset by eventual operations on this element...
	 */
	public void storeChanges(String table, String id,
			ColumnFamilyData changed,
			Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> incremented,
			Long ttlMs) {
		
		IllegalArgumentException x = null;
		
		Row r;
		do {
			r = this.getRow(table, id, true);
		} while (r == null || !r.startUpdate(ttlMs));
		
		long transaction = this.nextTransactionId.incrementAndGet();
		
		try {
		
			if (changed != null)
				for (Entry<String, Map<String, byte[]>> change : changed.entrySet()) {
					ColumnFamily f = r.get(change.getKey());
					for (Entry<String, byte[]> value : change.getValue().entrySet()) {
						Value<?> val = f.get(value.getKey());
						if (val instanceof ByteValue) {
							((ByteValue)val).setValue(value.getValue(), transaction);
						} else {
							x = new IllegalArgumentException("Cannot set an incrementing value " + value.getKey() + " in family " + change.getKey() + " for row " + id + " in table " + table);
						}
					}
				}
			
			if (removed != null)
				for (Entry<String, Set<String>> remove : removed.entrySet()) {
					ColumnFamily f = r.getNoCreate(remove.getKey());
					if (f != null) {
						for (String qual : remove.getValue()) {
							Value<?> val = f.getNoCreate(qual);
							if (val instanceof ByteValue) {
								((ByteValue)val).setValue(DELETED_VALUE, transaction);
							} else {
								x = new IllegalArgumentException("Cannot remove an incrementing value " + qual + " in family " + remove.getKey() + " for row " + id + " in table " + table);
							}
						}
					}
				}
			
			if (incremented != null)
				for (Entry<String, Map<String, Number>> incr : incremented.entrySet()) {
					ColumnFamily f = r.get(incr.getKey());
					for (Entry<String, Number> entry : incr.getValue().entrySet()) {
						f.incr(entry.getKey(), entry.getValue());
					}
				}
			
			if (x != null)
				throw x;
		} finally {
			r.stopUpdate();
		}
	}

	@Override
	public long count(String table, Constraint c)
			throws DatabaseNotReachedException {
		Table t = this.getTable(table, false);
		return t == null ? 0 : subMap(t.getNavigableMap(), c == null ? null : c.getStartKey(), c == null ? null : c.getEndKey()).size();
	}
	
	public void reset() {
		this.tables.clear();
	}

	@Override
	public void delete(String table, String id) {
		Table t = this.getTable(table, false);
		if (t != null)
			t.remove(id);
	}

	@Override
	public boolean exists(String table, String row, String family)
			throws DatabaseNotReachedException {
		ColumnFamily fam = this.getFamily(table, row, family, false);
		return fam != null && !fam.getValues(null, null).isEmpty();
	}

	@Override
	public boolean exists(String table, String row)
			throws DatabaseNotReachedException {
		if (!this.tables.contains(table))
			return false;
		return this.getRow(table, row, false) != null;
	}

	@Override
	public ColumnFamilyData get(String table, String id, Set<String> families) throws DatabaseNotReachedException {
		Row row = this.getRow(table, id, false);
		if (row == null)
			return null;
		
		ColumnFamilyData ret = new DefaultColumnFamilyData();
		
		for (String family : families) {
			ColumnFamily fam = row.getNoCreate(family);
			if (fam != null) {
				ret.put(family, fam.getValues(null, null));
			}
		}
		
		return ret;
	}

	@Override
	public CloseableKeyIterator get(final String table, Constraint c, final int limit, Set<String> families)
			throws DatabaseNotReachedException {
		Table t = this.getTable(table, false);
		if (t == null)
			return new EmptyCloseableIterator();
		final String endKey = c == null ? null : c.getEndKey();
		final Iterator<Row> ret = t.getRowIterator(c == null ? null : c.getStartKey());
		return new CloseableKeyIterator() {
			private int count = 0;
			private Row next = null;
			private boolean done;
			
			private Row updateNext() {
				if (next != null)
					return next;
				
				if (done) {
					return next = null;
				}
				
				if (++count > limit) {
					done = true;
					return next = null;
				}
				
				next = ret.hasNext() ? ret.next() : null;
				
				if (next == null
						|| (endKey != null && next.getKey().compareTo(endKey) > 0) ) {
					done = true;
					next = null;
				}
					
				return next;
			}	
			
			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
			@Override
			public com.googlecode.n_orm.storeapi.Row next() {
				com.googlecode.n_orm.storeapi.Row ret = updateNext();
				next = null;
				return ret;
			}
			
			@Override
			public boolean hasNext() {
				return this.updateNext() != null;
			}
			
			@Override
			public void close() {
			}
		}; 
	}
	
	/**
	 * Adds a listener to the eviction queue.
	 * In case the eviction queue does not exists, creates and starts it.
	 * @param l the listener that will be notified {@link EvictionListener#rowEvicted(com.googlecode.n_orm.storeapi.Row)}
	 * after a row is evicted.
	 */
	public void addEvictionListener(EvictionListener l) {
		getEvictionQueue().addEvictionListener(l);
	}
	
	/**
	 * Returns the eviction queue ; sets it up and starts it in case it does not already exists.
	 */
	private EvictionQueue getEvictionQueue() {
		EvictionQueue eq = this.evictionQueue.get();
		if (eq == null) {
			//It looks like the first time an eviction is set on this store...
			EvictionQueue tmp = new EvictionQueue();
			if (this.evictionQueue.compareAndSet(null, tmp)) {
				//Indeed
				tmp.start();
				eq = tmp;
			}
		}
		return eq;
	}

}
