package com.googlecode.n_orm.memory;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.memory.Memory.Table.Row;
import com.googlecode.n_orm.memory.Memory.Table.Row.ColumnFamily;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.Store;

/**
 * Reference implementation for a store.
 * This store entirely resides into memory, and is only available for the current JVM.
 * It is well suited for testing.
 * This store is thread-safe.
 */
public class Memory implements Store {
	public static final Memory INSTANCE = new Memory();
	
	private static class RowResult implements com.googlecode.n_orm.storeapi.Row, Comparable<RowResult> {
		private final Map<String, Map<String, byte[]>> values;
		private final String row;

		private RowResult(Map<String, Map<String, byte[]>> values, String row) {
			this.values = values;
			this.row = row;
		}

		@Override
		public Map<String, Map<String, byte[]>> getValues() {
			return values;
		}

		@Override
		public String getKey() {
			return row;
		}

		@Override
		public int compareTo(RowResult o) {
			return this.getKey().compareTo(o.getKey());
		}
	}

	@SuppressWarnings("serial")
	private abstract class LazyHash<T> {
		protected ReentrantReadWriteLock generalLock = new ReentrantReadWriteLock();
		protected Map<String, T> map = new TreeMap<String, T>();
		private Map<String, ReentrantReadWriteLock> locks = new TreeMap<String, ReentrantReadWriteLock>();
		
		
		protected abstract T newElement(String key);
		
		protected ReentrantReadWriteLock getLock(String key) {
			ReentrantReadWriteLock ret = locks.get(key);
			if (ret == null) {
				synchronized (locks) {
					ret = locks.get(key);
					if (ret == null) {
						ret = new ReentrantReadWriteLock();
						locks.put(key, ret);
					}
				}
			}
			return ret;
		}

		public final boolean contains(String key) {
			return map.containsKey(key);
		}

		public final T get(String key) {
			T ret = this.getNoCreate(key);
			if (ret != null)
				return ret;

			ReentrantReadWriteLock lock = this.getLock(key);
			lock.writeLock().lock();
			try {
				ret = map.get(key);
				if (ret == null) {
					ret = this.newElement((String) key);
					map.put((String) key, ret);
				}
				return ret;
			} finally {
				lock.writeLock().unlock();
			}
		}
		
		protected final T getNoCreate(String key) {
			ReentrantReadWriteLock lock = this.getLock(key);
			lock.readLock().lock();
			try {
				return map.get(key);
			} finally {
				lock.readLock().unlock();
			}
		}
		
		public final T put(String key, T value) {
			generalLock.readLock().lock();
			ReentrantReadWriteLock lock = this.getLock(key);
			lock.writeLock().lock();
			try {
				return map.put(key, value);
			} finally {
				generalLock.readLock().unlock();
				lock.writeLock().unlock();
			}
		}
		
		public final T remove(String key) {
			synchronized(map) {
				if (! this.contains(key)) {
					return null;
				}
			}
			ReentrantReadWriteLock lock = this.getLock(key);
			lock.writeLock().lock();
			generalLock.readLock().lock();
			try {
				return map.remove(key);
			} finally {
				generalLock.readLock().unlock();
				lock.writeLock().unlock();
				synchronized(locks) {
					locks.remove(key);
				}
			}
		}
		
		public final Set<String> getKeys() {
			generalLock.writeLock().lock();
			try {
				return new TreeSet<String>(map.keySet());
			} finally {
				generalLock.writeLock().unlock();
			}
		}
		
		public final Map<String, T> toMap() {
			Map<String, T> ret = new TreeMap<String, T>();
			for (String key : this.getKeys()) {
				T val = this.getNoCreate(key);
				if (val != null)
					ret.put(key, val);
			}
			return ret;
		}

		public void clear() {
			for (String key : this.getKeys()) {
				this.remove(key);
			}
		}

		public int size() {
			return this.map.size();
		}
		
		public boolean isEmpty() {
			return this.map.size() == 0;
		}
	}

	@SuppressWarnings("serial")
	public class Table extends LazyHash<Table.Row> {
		public class Row extends LazyHash<Row.ColumnFamily> implements com.googlecode.n_orm.storeapi.Row {
			public final String key;
			
			public Row(String key) {
				this.key = key;
			}

			public class ColumnFamily extends LazyHash<byte[]> {
				public final String name;
				
				public ColumnFamily(String name) {
					this.name = name;
				}

				@Override
				protected byte[] newElement(String key) {
					return new byte[0];
				}

				public synchronized void incr(String key, Number increment) {
					assert increment.longValue() != 0 : "Received a 0 increment for table" + Table.this.name + ", row " + Row.this.key + ", family " + this.name + ", qualifier " + key; 
					generalLock.readLock().lock();
					ReentrantReadWriteLock lock = this.getLock(key);
					lock.writeLock().lock();
					try {
						byte [] val = this.map.get(key);
						if (val == null) {
							this.map.put(key, ConversionTools.convert(increment, Number.class));
							return;
						}
						switch (val.length) {
						case 0:
							val = ConversionTools.convert(increment, Number.class);
							break;
						case Byte.SIZE/Byte.SIZE:
							val = ConversionTools.convert((byte)(ConversionTools.convert(byte.class, val)+increment.byteValue()), byte.class);
							break;
						case Short.SIZE/Byte.SIZE:
							val = ConversionTools.convert((short)(ConversionTools.convert(short.class, val)+increment.shortValue()), short.class);
							break;
						case Integer.SIZE/Byte.SIZE:
							val = ConversionTools.convert((int)(ConversionTools.convert(int.class, val)+increment.intValue()), int.class);
							break;
						case Long.SIZE/Byte.SIZE:
							val = ConversionTools.convert((long)(ConversionTools.convert(long.class, val)+increment.longValue()), long.class);
							break;
						default:
							assert false : "Unknown natural format: " + val.length*8 + " bytes.";	
						}
						this.map.put(key, val);
					} finally {
						generalLock.readLock().unlock();
						lock.writeLock().unlock();
					}
				}
				
			}

			@Override
			protected ColumnFamily newElement(String key) {
				return new ColumnFamily(key);
			}

			@Override
			public String getKey() {
				return key;
			}

			@Override
			public Map<String, Map<String, byte[]>> getValues() {
				Map<String, Map<String, byte[]>> ret = new TreeMap<String, Map<String,byte[]>>();
				for (String key : this.getKeys()) {
					ColumnFamily cf = this.get(key);
					if (cf != null) {
						ret.put(key, cf.toMap());
					}
				}
				return ret;
			}
		}
		
		public final String name;
		
		public Table(String name) {
			this.name = name;
		}

		protected Row newElement(String key) {
			return new Row(key);
		}
	}
	
	private LazyHash<Table> tables = new LazyHash<Table>() {

		@Override
		protected Table newElement(String key) {
			return new Table(key);
		}
		
	};
	
	private Memory() {}

	@Override
	public void start() {
	}
	
	public Table getTable(String table) {
		return this.tables.get(table);
	}

	@Override
	public byte[] get(String table, String id, String family, String key) {
		Table t = this.getTable(table);
		if (!t.contains(id))
			return null;
		Table.Row.ColumnFamily fam = this.getTable(table).get(id).get(family);
		return fam.contains(key) ? fam.get(key) : null;
	}

	@Override
	public Map<String, byte[]> get(String table, String id, String family) {
		Table t = this.getTable(table);
		if (!t.contains(id))
			return null;
		return this.getTable(table).get(id).get(family).toMap();
	}
	
	@Override
	public Map<String, byte[]> get(String table, String id, String family,
			Constraint c) throws DatabaseNotReachedException {
		Table t = this.getTable(table);
		if (!t.contains(id))
			return null;
		ColumnFamily cf = this.getTable(table).get(id).get(family);
		
		Map<String, byte[]> ret = new TreeMap<String, byte[]>();
		
		for (String key : matchingKeys(cf.getKeys(), c, null)) {
			ret.put(key, cf.get(key));
		}
		
		return ret;
	}

	@Override
	public void storeChanges(String table, String id,
			Map<String, Map<String, byte[]>> changed,
			Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> incremented) {
		Row r = this.getTable(table).get(id);
		if (r == null) {
			r = this.getTable(table).new Row(id);
			this.getTable(table).put(id, r);
		}
		for (String family : changed.keySet()) {
			ColumnFamily f = r.get(family);
			Map<String, byte[]> values = changed.get(family);
			for (String key : values.keySet()) {
				f.put(key, values.get(key));
			}
		}
		
		for (String family : removed.keySet()) {
			ColumnFamily f = r.get(family);
			for (String key : removed.get(family)) {
				f.remove(key);
			}
		}
		
		for (Entry<String, Map<String, Number>> family : incremented.entrySet()) {
			ColumnFamily f = r.get(family.getKey());
			Map<String, Number> incrs = family.getValue();
			for (Entry<String, Number> entry : incrs.entrySet()) {
				f.incr(entry.getKey(), entry.getValue());
			}
		}
	}

	public int count(String table) {
		return this.getTable(table).size();
	}

	public int count(String table, String row, String family) {
		Table t = this.getTable(table);
		if (!t.contains(row))
			return 0;
		return this.getTable(table).get(row).get(family).size();
	}
	
	public void reset() {
		this.tables.clear();
	}

//	public int count(String ownerTable, String identifier, String name,
//			Constraint c) throws DatabaseNotReachedException {
//		Table t = this.getTable(ownerTable);
//		if (!t.containsKey(identifier))
//			return 0;
//		return this.get(ownerTable, identifier, name, c).size();
//	}

	@Override
	public void delete(String table, String id) {
		this.getTable(table).remove(id);
	}

	public boolean exists(String table, String row, String family, String key)
			throws DatabaseNotReachedException {
		if (!this.exists(table, row))
			return false;
		Row r = this.getTable(table).get(row);
		if (!r.contains(family))
			return false;
		ColumnFamily cf = r.get(family);
		return cf.contains(key);
	}

	@Override
	public boolean exists(String table, String row, String family)
			throws DatabaseNotReachedException {
		if (!this.exists(table, row))
			return false;
		Row r = this.getTable(table).get(row);
		if (!r.contains(family))
			return false;
		ColumnFamily cf = r.get(family);
		return !cf.isEmpty();
	}

	public boolean exists(String table, String row)
			throws DatabaseNotReachedException {
		if (!this.tables.contains(table))
			return false;
		Table t = this.getTable(table);
		return t.contains(row);
	}
	
	protected Set<String> matchingKeys(Set<String> keys, Constraint c, Integer maxNmber) {
		Set<String> ret = new TreeSet<String>();
		for (String key : keys) {
			if (c == null || ((c.getStartKey() == null || c.getStartKey().compareTo(key) <= 0) && (c.getEndKey() == null || key.compareTo(c.getEndKey()) <= 0))) {
				ret.add(key);
				if (maxNmber != null && ret.size() >= maxNmber)
					return ret;
			}
		}
		return ret;
	}

	@Override
	public Map<String, Map<String, byte[]>> get(String table, String id, Set<String> families) throws DatabaseNotReachedException {

		Table t = this.getTable(table);
		if (t == null || !t.contains(id))
			return null;
		Row row = t.get(id);
		
		Map<String, Map<String, byte[]>> ret = new TreeMap<String, Map<String,byte[]>>();
		
		for (String family : families) {
			if (row.contains(family)) {
				ret.put(family, new TreeMap<String, byte[]>(row.get(family).toMap()));
			}
		}
		
		return ret;
	}

	private Set<com.googlecode.n_orm.storeapi.Row> getRows(final String table,
			Constraint c, Integer limit, Set<String> families) {
		Table t = this.getTable(table);
		Set<String> res = this.matchingKeys(t.getKeys(), c, limit);
		Set<com.googlecode.n_orm.storeapi.Row> rows = new TreeSet<com.googlecode.n_orm.storeapi.Row>();
		for (final String row : res) {
			final Map<String, Map<String, byte[]>> values = new TreeMap<String, Map<String,byte[]>>();
			if (families != null) {
				Row memRow = t.get(row);
				for (String fams : memRow.getKeys()) {
					if (families.contains(fams)) {
						ColumnFamily memFam = memRow.get(fams);
						if (memFam != null)
							values.put(fams, memFam.toMap());
						else
							values.put(fams, new TreeMap<String, byte[]>());
					}
				}
			}
			rows.add(new RowResult(values, row));
		}
		return rows;
	}

	@Override
	public CloseableKeyIterator get(final String table, Constraint c, int limit, Set<String> families)
			throws DatabaseNotReachedException {
		Set<com.googlecode.n_orm.storeapi.Row> rows = getRows(table, c, limit,
				families);
		final Iterator<com.googlecode.n_orm.storeapi.Row> ret = rows.iterator();
		return new CloseableKeyIterator() {
			
			@Override
			public void remove() {
				throw new IllegalStateException("Cannot remove a key like this...");
			}
			
			@Override
			public com.googlecode.n_orm.storeapi.Row next() {
				return ret.next();
			}
			
			@Override
			public boolean hasNext() {
				return ret.hasNext();
			}
			
			@Override
			public void close() {
			}
		}; 
	}

	@Override
	public long count(String table, Constraint c)
			throws DatabaseNotReachedException {
		Table t = this.getTable(table);
		return this.matchingKeys(t.getKeys(), c, null).size();
	}

//	@Override
//	public void truncate(String table, Constraint c)
//			throws DatabaseNotReachedException {
//		Table t = this.getTable(table);
//		for (String r : this.matchingKeys(t.keySet(), c, null)) {
//			t.remove(r);
//		}
//	}

}
