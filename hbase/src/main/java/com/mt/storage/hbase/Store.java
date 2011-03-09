package com.mt.storage.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.mt.storage.CloseableKeyIterator;
import com.mt.storage.Constraint;
import com.mt.storage.DatabaseNotReachedException;

public class Store implements com.mt.storage.GenericStore {

	private static final class CloseableIterator implements Iterator<String>,
			CloseableKeyIterator {
		private final ResultScanner result;
		private final Iterator<Result> iterator;

		private CloseableIterator(ResultScanner res) {
			this.result = res;
			this.iterator = res.iterator();
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public String next() {
			return Bytes.toString(iterator.next().getRow());
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
		 * @see com.mt.storage.hbase.CloseableIterator#close()
		 */
		@Override
		public void close() {
			result.close();
		}
	}

	protected static Map<Properties, Store> knownStores = new HashMap<Properties, Store>();

	public static Store getStore(String host, int port) {
		return getStore(host, port, null);
	}

	public static Store getStore(String host, int port, Integer maxRetries) {
		synchronized (Store.class) {
			Properties p = new Properties();
			p.setProperty("host", host);
			p.setProperty("port", Integer.toString(port));
			if (maxRetries != null)
				p.setProperty("maxRetries", maxRetries.toString());
			Store ret = knownStores.get(p);
			if (ret == null) {
				ret = new Store();
				ret.setHost(host);
				ret.setPort(port);
				if (maxRetries != null)
					ret.setMaxRetries(maxRetries);
				knownStores.put(p, ret);
			}
			return ret;
		}
	}

	private String host = "localhost";
	private int port = HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;
	private Integer maxRetries = null;
	private boolean wasStarted = false;
	private Configuration config;
	private HBaseAdmin admin;
	public Map<String, HTableDescriptor> tablesD = new TreeMap<String, HTableDescriptor>();
	public Map<String, HTable> tablesC = new TreeMap<String, HTable>();

	protected Store() {
	}

	public synchronized void start() throws DatabaseNotReachedException {
		if (this.wasStarted)
			return;

		if (this.config == null) {
			Configuration properties = HBaseConfiguration.create();
			properties.clear();

			properties.set(HConstants.ZOOKEEPER_QUORUM, this.getHost());
			properties.setInt("hbase.zookeeper.property.clientPort",
					this.getPort());

			if (this.maxRetries != null)
				properties.set("hbase.client.retries.number",
						this.maxRetries.toString());

			this.config = properties;
		}

		if (this.admin == null)
			try {
				this.admin = new HBaseAdmin(this.config);
				if (!this.admin.isMasterRunning())
					throw new DatabaseNotReachedException(
							new MasterNotRunningException());
			} catch (Exception e) {
				throw new DatabaseNotReachedException(e);
			}

		this.wasStarted = true;
	}

	public String getHost() {
		return host;
	}

	@Override
	public void setHost(String url) {
		this.host = url;
	}// }

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setMaxRetries(int maxRetries) {
		this.maxRetries = Integer.valueOf(maxRetries);
	}

	public Configuration getConf() {
		return this.config;
	}

	public void setConf(Configuration configuration) {
		this.config = configuration;
	}
	
	public HBaseAdmin getAdmin() {
		return this.admin;
	}

	public void setAdmin(HBaseAdmin admin) {
		this.admin = admin;
		this.setConf(admin.getConfiguration());
	}
	
	protected String mangleTableName(String table) {
		if (table.startsWith(".") || table.startsWith("-")) {
			table = "t" + table;
		}
		
		for (int i = 0; i < table.length(); i++) {
			char c = table.charAt(i);
			if (! (Character.isLetterOrDigit(c) || c == '_' || c == '-' || c == '.'))
				table = table.substring(0, i) + '_' + table.substring(i+1);
		}
		
		return table;
	}

	protected boolean hasTable(String name) throws DatabaseNotReachedException {
		name = this.mangleTableName(name);
		if (this.tablesD.containsKey(name))
			return true;

		try {
			return this.admin.tableExists(name);
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}
	}

	protected HTable getTable(String name, Set<String> expectedFamilies)
			throws DatabaseNotReachedException {
		name = this.mangleTableName(name);
		HTableDescriptor td;
		boolean created = false;
		synchronized (this.tablesD) {
			if (!this.tablesD.containsKey(name)) {
				try {
					if (!this.admin.tableExists(name)) {
						td = new HTableDescriptor(name);
						if (expectedFamilies != null) {
							for (String fam : expectedFamilies) {
								td.addFamily(new HColumnDescriptor(fam));
							}
						}
						this.admin.createTable(td);
						created = true;
					} else {
						td = this.admin.getTableDescriptor(Bytes.toBytes(name));
					}
					this.tablesD.put(name, td);
				} catch (IOException e) {
					throw new DatabaseNotReachedException(e);
				}
			} else
				td = this.tablesD.get(name);
		}

		if (!created && expectedFamilies != null) {
			this.enforceColumnFamiliesExists(td, expectedFamilies);
		}

		synchronized (this.tablesC) {
			HTable ret = this.tablesC.get(name);
			if (ret == null) {
				try {
					ret = new HTable(this.config, name);
					this.tablesC.put(name, ret);
					return ret;
				} catch (IOException e) {
					throw new DatabaseNotReachedException(e);
				}
			}
			return ret;
		}
	}

	protected boolean hasColumnFamily(String table, String family)
			throws DatabaseNotReachedException {
		table = this.mangleTableName(table);
		if (!this.hasTable(table))
			return false;

		HTableDescriptor td;
		synchronized (this.tablesD) {
			td = this.tablesD.get(table);
		}
		if (td.hasFamily(Bytes.toBytes(family)))
			return true;
		synchronized (this.tablesD) {
			try {
				td = this.admin.getTableDescriptor(td.getName());
				this.tablesD.put(table, td);
			} catch (IOException e) {
				throw new DatabaseNotReachedException(e);
			}
		}
		return td.hasFamily(Bytes.toBytes(family));
	}

	private void enforceColumnFamiliesExists(HTableDescriptor tableD,
			Set<String> columnFamilies) throws DatabaseNotReachedException {
		assert tableD != null;
		List<HColumnDescriptor> toBeAdded = new ArrayList<HColumnDescriptor>(
				columnFamilies.size());
		synchronized (tableD) {
			boolean recreated = false;
			for (String cf : columnFamilies) {
				byte[] cfname = Bytes.toBytes(cf);
				if (!recreated && !tableD.hasFamily(cfname)) {
					try {
						tableD = this.admin
								.getTableDescriptor(tableD.getName());
						this.tablesD.put(tableD.getNameAsString(), tableD);
						recreated = true;
					} catch (IOException e) {
						throw new DatabaseNotReachedException(e);
					}
				}
				if (!tableD.hasFamily(cfname)) {
					HColumnDescriptor newFamily = new HColumnDescriptor(cfname);
					toBeAdded.add(newFamily);
					tableD.addFamily(newFamily);
				}
			}
			if (!toBeAdded.isEmpty()) {
				try {
					this.admin.disableTable(tableD.getName());
					for (HColumnDescriptor hColumnDescriptor : toBeAdded) {
						this.admin.addColumn(tableD.getName(),
								hColumnDescriptor);
					}
					this.admin.enableTable(tableD.getName());
				} catch (IOException e) {
					throw new DatabaseNotReachedException(e);
				}

			}
		}
	}

	@Override
	public Map<String, Map<String, byte[]>> get(String table, String id,
			Set<String> families) throws DatabaseNotReachedException {
		HTable t = this.getTable(table, families);

		Get g = new Get(Bytes.toBytes(id));
		for (String family : families) {
			g.addFamily(Bytes.toBytes(family));
		}

		Result r;
		try {
			r = t.get(g);
		} catch (IOException e) {
			throw new DatabaseNotReachedException(e);
		}
		Map<String, Map<String, byte[]>> ret = new TreeMap<String, Map<String, byte[]>>();
		if (!r.isEmpty()) {
			for (KeyValue kv : r.list()) {
				String family = Bytes.toString(kv.getFamily());
				if (!ret.containsKey(family))
					ret.put(family, new TreeMap<String, byte[]>());
				ret.get(family).put(Bytes.toString(kv.getQualifier()),
						kv.getValue());
			}
		}
		return ret;
	}

	protected Filter createFamilyConstraint(Constraint c,
			boolean firstKeyOnly) {
		Filter f = null;
		if (c.getStartKey() != null)
			f = new QualifierFilter(CompareOp.GREATER_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes(c.getStartKey())));
		if (c.getEndKey() != null)
			f = this.addFilter(f, new QualifierFilter(CompareOp.LESS_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes(c.getEndKey()))));
		if (firstKeyOnly)
			f = this.addFilter(f, new FirstKeyOnlyFilter());
		return f;
	}

	private Filter addFilter(Filter f1, Filter f2) {
		if (f2 == null) {
			return f1;
		} else if (f1 == null) {
			return f2;
		} else if (f1 instanceof FilterList) {
			((FilterList) f1).addFilter(f2);
			return f1;
		} else {
			FilterList list = new FilterList();
			list.addFilter(f1);
			list.addFilter(f2);
			return list;
		}
	}

	@Override
	public void storeChanges(String table, String id,
			Map<String, Map<String, byte[]>> changed,
			Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> increments)
			throws DatabaseNotReachedException {
		Set<String> families = new HashSet<String>(changed.keySet());
		families.addAll(removed.keySet());
		families.addAll(increments.keySet());
		HTable t = this.getTable(table, families);

		byte[] row = Bytes.toBytes(id);

		Put rowPut = null;

		if (changed != null && !changed.isEmpty()) {
			rowPut = new Put(row);
			for (String family : changed.keySet()) {
				byte[] cf = Bytes.toBytes(family);
				Map<String, byte[]> toPut = changed.get(family);
				for (String key : toPut.keySet()) {
					rowPut.add(cf, Bytes.toBytes(key), toPut.get(key));
				}
			}
		}

		Delete rowDel = null;
		if (removed != null && !removed.isEmpty()) {
			rowDel = new Delete(row);
			for (String family : removed.keySet()) {
				byte[] cf = Bytes.toBytes(family);
				for (String key : removed.get(family)) {
					rowDel.deleteColumns(cf, Bytes.toBytes(key));
				}

			}
		}

		IOException problem = null;

		try {
			if (rowPut != null)
				t.put(rowPut);
		} catch (IOException e) {
			problem = e;
		}
		try {
			if (rowDel != null)
				t.delete(rowDel);
		} catch (IOException e) {
			if (problem == null)
				problem = e;
		}

		try {
			for (String fam : increments.keySet()) {
				Map<String, Number> incr = increments.get(fam);
				for (String key : incr.keySet()) {
					t.incrementColumnValue(row, Bytes.toBytes(fam),
							Bytes.toBytes(key), incr.get(key).longValue());
				}
			}
		} catch (IOException e) {
			if (problem == null)
				problem = e;
		}

		if (problem != null)
			throw new DatabaseNotReachedException(problem);
	}

	@Override
	public void delete(String table, String id)
			throws DatabaseNotReachedException {
		HTable t = this.getTable(table, null);

		Delete rowDel = new Delete(Bytes.toBytes(id));
		try {
			t.delete(rowDel);
		} catch (IOException e) {
			throw new DatabaseNotReachedException(e);
		}
	}

	// @Override
	// public int count(String table) throws DatabaseNotReachedException {
	// if (!this.hasTable(table))
	// return 0;
	//
	// HTable t = this.getTable(table, null);
	// Scan s = new Scan();
	// s.setFilter(new FirstKeyOnlyFilter());
	//
	// try {
	// int ret = 0;
	// Iterator<?> res = t.getScanner(s).iterator();
	// while(res.hasNext()) {
	// ret++;
	// res.next();
	// }
	// return ret;
	// } catch (IOException e) {
	// throw new DatabaseNotReachedException(e);
	// }
	// }

	// @Override
	// public int count(String table, String row, String family) throws
	// DatabaseNotReachedException {
	// if (!this.hasTable(table))
	// return 0;
	//
	// HTable t = this.getTable(table, null);
	// Get g = new Get(Bytes.toBytes(row)).addFamily(Bytes.toBytes(family));
	//
	// try {
	// int ret = 0;
	// Iterator<?> res = t.get(g).iterator();
	// while(res.hasNext()) {
	// ret++;
	// res.next();
	// }
	// return ret;
	// } catch (IOException e) {
	// throw new DatabaseNotReachedException(e);
	// }
	// }

	// @Override
	// public int count(String table, String row, String family,
	// Constraint c) throws DatabaseNotReachedException {String family, 
	// if (!this.hasTable(table))
	// return 0;
	//
	// HTable t = this.getTable(table, null);
	// Scan s = new Scan(new
	// Get(Bytes.toBytes(row)).addFamily(Bytes.toBytes(family)));
	// s.setFilter(this.createFamilyConstraint(c, false));
	//
	// try {
	// int ret = 0;
	// Iterator<?> res = t.getScanner(s).iterator();
	// while(res.hasNext()) {
	// ret++;
	// res.next();
	// }
	// return ret;
	// } catch (IOException e) {
	// throw new DatabaseNotReachedException(e);
	// }
	// }

	// @Override
	// public boolean exists(String table, String row, String family, String
	// key)
	// throws DatabaseNotReachedException {
	// HashSet<String> fam = new HashSet<String>();String family, 
	// fam.add(family);
	// HTable t = this.getTable(table, fam);
	// Get g = new
	// Get(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family),Bytes.toBytes(key));
	// g.setFilter(new FirstKeyOnlyFilter());
	//
	// try {
	// return t.exists(g);
	// } catch (IOException e) {
	// throw new DatabaseNotReachedException(e);
	// }
	// }

	@Override
	public boolean exists(String table, String row, String family)
			throws DatabaseNotReachedException {
		if (!this.hasTable(table))
			return false;
		if (!this.hasColumnFamily(table, family))
			return false;

		HTable t = this.getTable(table, null);
		Get g = new Get(Bytes.toBytes(row)).addFamily(Bytes.toBytes(family));

		try {
			return t.exists(g);
		} catch (IOException e) {
			throw new DatabaseNotReachedException(e);
		}
	}

	@Override
	public boolean exists(String table, String row)
			throws DatabaseNotReachedException {
		if (!this.hasTable(table))
			return false;

		HTable t = this.getTable(table, null);
		Get g = new Get(Bytes.toBytes(row));
		g.setFilter(new FirstKeyOnlyFilter());

		try {
			return t.exists(g);
		} catch (IOException e) {
			throw new DatabaseNotReachedException(e);
		}
	}

	@Override
	public byte[] get(String table, String row, String family, String key)
			throws DatabaseNotReachedException {
		HashSet<String> fam = new HashSet<String>();
		fam.add(family);
		HTable t = this.getTable(table, fam);
		Get g = new Get(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family),
				Bytes.toBytes(key));

		try {
			Result result = t.get(g);
			if (result.isEmpty())
				return null;
			return result.value();
		} catch (IOException e) {
			throw new DatabaseNotReachedException(e);
		}
	}

	@Override
	public Map<String, byte[]> get(String table, String id, String family)
			throws DatabaseNotReachedException {
		return this.get(table, id, family, (Constraint) null);
	}

	@Override
	public Map<String, byte[]> get(String table, String id, String family,
			Constraint c) throws DatabaseNotReachedException {
		Set<String> families = new TreeSet<String>();
		families.add(family);
		HTable t = this.getTable(table, families);

		Get g = new Get(Bytes.toBytes(id)).addFamily(Bytes.toBytes(family));

		if (c != null) {
			g.setFilter(createFamilyConstraint(c, false));
		}

		Result r;
		try {
			r = t.get(g);
		} catch (IOException e) {
			throw new DatabaseNotReachedException(e);
		}
		Map<String, byte[]> ret = new HashMap<String, byte[]>();
		if (!r.isEmpty()) {
			for (KeyValue kv : r.list()) {
				ret.put(Bytes.toString(kv.getQualifier()), kv.getValue());
			}
		}
		return ret;
	}

	@Override
	public com.mt.storage.CloseableKeyIterator get(String table, Constraint c,
			int limit) throws DatabaseNotReachedException {
		if (!this.hasTable(table))
			return new com.mt.storage.CloseableKeyIterator() {

				@Override
				public void close() {
				}

				@Override
				public boolean hasNext() {
					return false;
				}

				@Override
				public String next() {
					return null;
				}

				@Override
				public void remove() {
				}

			};

		HTable ht = this.getTable(table, null);

		Scan s = new Scan();
		if (c != null && c.getStartKey() != null)
			s.setStartRow(Bytes.toBytes(c.getStartKey()));
		if (c != null && c.getEndKey() != null) {
			byte[] endb = Bytes.toBytes(c.getEndKey());
			endb = Bytes.add(endb, new byte[] { 0 });
			s.setStopRow(endb);
		}
		s.setFilter(new PageFilter(limit));

		final ResultScanner r;
		try {
			r = ht.getScanner(s);
		} catch (IOException e) {
			throw new DatabaseNotReachedException(e);
		}
		return new CloseableIterator(r);
	}

}