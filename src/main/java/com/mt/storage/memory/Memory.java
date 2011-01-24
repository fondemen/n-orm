package com.mt.storage.memory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.mt.storage.CloseableKeyIterator;
import com.mt.storage.Constraint;
import com.mt.storage.DatabaseNotReachedException;
import com.mt.storage.Store;
import com.mt.storage.conversion.ConversionTools;
import com.mt.storage.memory.Memory.Table.Row;
import com.mt.storage.memory.Memory.Table.Row.ColumnFamily;

public class Memory implements Store {
	public static final Memory INSTANCE = new Memory();
	
	@SuppressWarnings("serial")
	private abstract class LazyHash<T> extends HashMap<String, T> {
		protected abstract T newElement(String key);

		@Override
		public T get(Object key) {
			T ret = super.get(key);
			if (ret == null) {
				ret = this.newElement((String) key);
				this.put((String) key, ret);
			}
			return ret;
		}
		
		
	}

	@SuppressWarnings("serial")
	public class Table extends HashMap<String, Table.Row> {
		public class Row extends LazyHash<Row.ColumnFamily> {
			public class ColumnFamily extends LazyHash<byte[]> {

				@Override
				protected byte[] newElement(String key) {
					return new byte[0];
				}
				
			}

			@Override
			protected ColumnFamily newElement(String key) {
				return new ColumnFamily();
			}
		}

		protected Row newElement(String key) {
			return new Row();
		}
	}
	
	@SuppressWarnings("serial")
	private Map<String, Table> tables = new LazyHash<Table>() {

		@Override
		protected Table newElement(String key) {
			return new Table();
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
		if (!t.containsKey(id))
			return null;
		Table.Row.ColumnFamily fam = this.getTable(table).get(id).get(family);
		return fam.containsKey(key) ? fam.get(key) : null;
	}

	@Override
	public Map<String, byte[]> get(String table, String id, String family) {
		Table t = this.getTable(table);
		if (!t.containsKey(id))
			return new TreeMap<String, byte[]>();
		return this.getTable(table).get(id).get(family);
	}

	@Override
	public void storeChanges(String table, String id,
			Map<String, Map<String, byte[]>> changed,
			Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> incremented) {
		Row r = this.getTable(table).get(id);
		if (r == null) {
			r = this.getTable(table).new Row();
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
		
		for (String family : incremented.keySet()) {
			ColumnFamily f = r.get(family);
			Map<String, Number> incrs = incremented.get(family);
			for (String key : incrs.keySet()) {
				byte [] val = f.get(key);
				Number increment = incrs.get(key);
				switch (val.length) {
				case 0:
					val = ConversionTools.convert(increment);
					break;
				case Byte.SIZE/Byte.SIZE:
					val = ConversionTools.convert((byte)(ConversionTools.convert(byte.class, val)+increment.byteValue()));
					break;
				case Short.SIZE/Byte.SIZE:
					val = ConversionTools.convert((short)(ConversionTools.convert(short.class, val)+increment.shortValue()));
					break;
				case Integer.SIZE/Byte.SIZE:
					val = ConversionTools.convert((int)(ConversionTools.convert(int.class, val)+increment.intValue()));
					break;
				case Long.SIZE/Byte.SIZE:
					val = ConversionTools.convert((long)(ConversionTools.convert(long.class, val)+increment.longValue()));
					break;
				default:
					assert false : "Unknown natural format: " + val.length*8 + " bytes.";	
				}
				f.put(key, val);
			}
		}
	}

	public int count(String table) {
		return this.getTable(table).size();
	}

	public int count(String table, String row, String family) {
		Table t = this.getTable(table);
		if (!t.containsKey(row))
			return 0;
		return this.getTable(table).get(row).get(family).size();
	}
	
	public void reset() {
		this.tables.clear();
	}

	public int count(String ownerTable, String identifier, String name,
			Constraint c) throws DatabaseNotReachedException {
		Table t = this.getTable(ownerTable);
		if (!t.containsKey(identifier))
			return 0;
		return this.get(ownerTable, identifier, name, c).size();
	}

	@Override
	public void delete(String table, String id) {
		this.getTable(table).remove(id);
	}

	public boolean exists(String table, String row, String family, String key)
			throws DatabaseNotReachedException {
		if (!this.exists(table, row))
			return false;
		Row r = this.getTable(table).get(row);
		if (!r.containsKey(family))
			return false;
		ColumnFamily cf = r.get(family);
		return cf.containsKey(key);
	}

	@Override
	public boolean exists(String table, String row, String family)
			throws DatabaseNotReachedException {
		if (!this.exists(table, row))
			return false;
		Row r = this.getTable(table).get(row);
		if (!r.containsKey(family))
			return false;
		ColumnFamily cf = r.get(family);
		return !cf.isEmpty();
	}

	public boolean exists(String table, String row)
			throws DatabaseNotReachedException {
		if (!this.tables.containsKey(table))
			return false;
		Table t = this.getTable(table);
		return t.containsKey(row);
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
	public Map<String, byte[]> get(String table, String id, String family,
			Constraint c) throws DatabaseNotReachedException {
		Table t = this.getTable(table);
		if (!t.containsKey(id))
			return new TreeMap<String, byte[]>();
		ColumnFamily cf = this.getTable(table).get(id).get(family);
		
		Map<String, byte[]> ret = new HashMap<String, byte[]>();
		
		for (String key : matchingKeys(cf.keySet(), c, null)) {
			ret.put(key, cf.get(key));
		}
		
		return ret;
	}

	@Override
	public CloseableKeyIterator get(String table, Constraint c, int limit)
			throws DatabaseNotReachedException {
		final Iterator<String> res = this.matchingKeys(this.getTable(table).keySet(), c, limit).iterator();
		return new CloseableKeyIterator() {
			
			@Override
			public void remove() {
				throw new IllegalStateException("Cannot remove a key like this...");
			}
			
			@Override
			public String next() {
				return res.next();
			}
			
			@Override
			public boolean hasNext() {
				return res.hasNext();
			}
			
			@Override
			public void close() {
			}
		}; 
	}

}
