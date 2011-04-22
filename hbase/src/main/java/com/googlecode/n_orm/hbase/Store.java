package com.googlecode.n_orm.hbase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.hbase.RecursiveFileAction.Report;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.Row;

/**
 * The HBase store found according to its configuration folder.
 * An example store.properties file is:<br><code>
 * class=com.googlecode.n_orm.hbase.Store<br>
 * static-accessor=getStore<br>
 * 1=/usr/lib/hadoop-0.20/conf/,/usr/lib/hbase/conf/
 * </code><br>
 * For test purpose, you can also directly reach an HBase instance thanks to one of its zookeeper host and client port:<br><code>
 * class=com.googlecode.n_orm.hbase.Store<br>
 * static-accessor=getStore<br>
 * 1=localhost<br>
 * 2=2181
 * </code><br>  
 */
public class Store implements com.googlecode.n_orm.storeapi.GenericStore {

	private final class EmptyIterator implements
			com.googlecode.n_orm.storeapi.CloseableKeyIterator {
		@Override
		public void close() {
		}

		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public Row next() {
			return null;
		}

		@Override
		public void remove() {
		}
	}

	private static final class CloseableIterator implements CloseableKeyIterator {
		private final ResultScanner result;
		private final Iterator<Result> iterator;
		private final boolean sendValues;

		private CloseableIterator(ResultScanner res, boolean sendValues) {
			this.result = res;
			this.sendValues = sendValues;
			this.iterator = res.iterator();
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public Row next() {
			final Result r = iterator.next();
			final String key = Bytes.toString(r.getRow());
			final Map<String, Map<String, byte[]>> vals = this.sendValues ? new TreeMap<String, Map<String,byte[]>>() : null;
			if (this.sendValues) {
				for (Entry<byte[], NavigableMap<byte[], byte[]>> famData : r.getNoVersionMap().entrySet()) {
					Map<String, byte[]> fam = new TreeMap<String, byte[]>();
					vals.put(Bytes.toString(famData.getKey()), fam);
					for (Entry<byte[], byte[]> colData : famData.getValue().entrySet()) {
						fam.put(Bytes.toString(colData.getKey()), colData.getValue());
					}
				}
			}
			return new Row() {
				
				@Override
				public Map<String, Map<String, byte[]>> getValues() {
					return vals;
				}
				
				@Override
				public String getKey() {
					return key;
				}
			};
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
			result.close();
		}
	}
	
	private static class ReportConf extends Report {
		private final Configuration conf;
		public boolean foundPropertyFile = false;
		public boolean foundHBasePropertyFile = false;

		public ReportConf(Configuration conf) {
			super();
			this.conf = conf;
		}

		Configuration getConf() {
			return conf;
		}

		boolean isFoundPropertyFile() {
			return foundPropertyFile;
		}

		boolean isFoundHBasePropertyFile() {
			return foundHBasePropertyFile;
		}

		@Override
		public void fileHandled(File f) {
			this.foundPropertyFile = true;
			if (!this.foundHBasePropertyFile && f.getName().equals("hbase-site.xml"))
				this.foundHBasePropertyFile = true;
		}
		
	}

	private static RecursiveFileAction addConfAction = new RecursiveFileAction() {
		
		@Override
		public void manageFile(File f, Report r) {
			try {
				((ReportConf)r).getConf().addResource(new FileInputStream(f));
				System.out.println("Getting HBase store: found configuration file " + f.getAbsolutePath());
			} catch (FileNotFoundException e) {
				System.err.println("Could not load configuration file " + f.getName());
				e.printStackTrace();
			}
		}
		
		@Override
		public boolean acceptFile(File file) {
			return file.getName().endsWith("-site.xml");
		}
	};
	protected static Map<Properties, Store> knownStores = new HashMap<Properties, Store>();

	/**
	 * For test purpose ; avoid using this.
	 */
	public static Store getStore(String host, int port) {
		return getStore(host, port, null);
	}

	/**
	 * For test purpose ; avoid using this.
	 */
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
	
	/**
	 * Get an HBase store according to a set of comma-separated configuration folders.
	 * Those folders are supposed to have configuration files following the pattern *-site.xml. 
	 */
	public static Store getStore(String commaSeparatedConfigurationFolders) throws IOException {
		synchronized (Store.class) {
			Properties p = new Properties();
			p.setProperty("commaSeparatedConfigurationFolders", commaSeparatedConfigurationFolders);
			Store ret = knownStores.get(p);
			if (ret == null) {
				Configuration conf = new Configuration();
				ReportConf r = new ReportConf(conf);
				for (String  configurationFolder : commaSeparatedConfigurationFolders.split(",")) {
					
					File confd = new File(configurationFolder);
					if (!confd.isDirectory()) {
						System.err.println("Cannot read HBase configuration folder " + confd);
						continue;
					}
					
					addConfAction.recursiveManageFile(confd, r);
				}
			
				if (!r.foundPropertyFile)
					throw new IOException("No configuration file found in the following folders " + commaSeparatedConfigurationFolders + " ; expecting some *-site.xml files");
				if (!r.foundHBasePropertyFile)
					throw new IOException("Could not find hbase-site.xml from folders " + commaSeparatedConfigurationFolders);
				
				ret = new Store();
				ret.setConf(new HBaseConfiguration(conf));
				
				knownStores.put(p, ret);
			}
			return ret;
		}
	}

	private String host = "localhost";
	private int port = 2181;
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
			Configuration properties = new HBaseConfiguration();
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
				this.admin = new HBaseAdmin(new HBaseConfiguration(this.config));
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
		try {
			Field confProp = HBaseAdmin.class.getDeclaredField("conf");
			confProp.setAccessible(true);
			this.setConf((Configuration)confProp.get(admin));
		} catch (Exception x) {}
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
	
	private void cache(String tableName, HTableDescriptor descr) {
		synchronized(this.tablesD) {
			this.uncache(tableName);
			this.tablesD.put(tableName, descr);
		}
	}
	
	private void cache(String tableName, HTable t) {
		synchronized(this.tablesC) {
			this.tablesC.put(tableName, t);
		}
	}

	private void uncache(String tableName) {
		//tableName = this.mangleTableName(tableName);
		synchronized (this.tablesD) {
			synchronized (this.tablesC) {
				this.tablesD.remove(tableName);
				this.tablesC.remove(tableName);
			}
			
		}
	}

	private void uncache(HTable t) {
		String tableName = Bytes.toString(t.getTableName());
		this.uncache(tableName);
	}

	protected boolean hasTable(String name) throws DatabaseNotReachedException {
		name = this.mangleTableName(name);
		if (this.tablesD.containsKey(name))
			return true;

		return hasTableNoCache(name);
	}

	private boolean hasTableNoCache(String name) {
		name = this.mangleTableName(name);
		try {
			boolean ret = this.admin.tableExists(name);
			if (!ret &&this.tablesD.containsKey(name)) {
				this.uncache(name);
			}
			return ret;
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}
	}

	protected HTable getTable(String name, String... expectedFamilies)
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
					this.cache(name, td);
				} catch (IOException e) {
					throw new DatabaseNotReachedException(e);
				}
			} else {
				td = this.tablesD.get(name);
			}
		}

		if (!created && expectedFamilies != null) {
			this.enforceColumnFamiliesExists(td, expectedFamilies);
		}

		synchronized (this.tablesC) {
			HTable ret = this.tablesC.get(name);
			if (ret == null) {
				try {
					ret = new HTable((HBaseConfiguration)this.config, name);
					this.cache(name, ret);
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
		if (td != null && td.hasFamily(Bytes.toBytes(family)))
			return true;
		synchronized (this.tablesD) {
			try {
				td = this.admin.getTableDescriptor(Bytes.toBytes(table));
				this.cache(table, td);
			} catch (IOException e) {
				throw new DatabaseNotReachedException(e);
			}
		}
		return td.hasFamily(Bytes.toBytes(family));
	}

	private void enforceColumnFamiliesExists(HTableDescriptor tableD,
			String... columnFamilies) throws DatabaseNotReachedException {
		assert tableD != null;
		List<HColumnDescriptor> toBeAdded = new ArrayList<HColumnDescriptor>(
				columnFamilies.length);
		synchronized (tableD) {
			boolean recreated = false;
			for (String cf : columnFamilies) {
				byte[] cfname = Bytes.toBytes(cf);
				if (!recreated && !tableD.hasFamily(cfname)) {
					String tableName = tableD.getNameAsString();
					try {
						tableD = this.admin
								.getTableDescriptor(tableD.getName());
						this.cache(tableName, tableD);
						recreated = true;
					} catch (TableNotFoundException x) {
						//Table exists in the cache but was dropped for some reason...
						this.uncache(tableName);
						this.getTable(tableName, columnFamilies);
						return;
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
		HTable t = this.getTable(table, families.toArray(new String[families.size()]));

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
		if (changed == null)
			changed = new TreeMap<String, Map<String,byte[]>>();
		if (removed == null)
			removed = new TreeMap<String, Set<String>>();
		if (increments == null)
			increments = new TreeMap<String, Map<String,Number>>();
		Set<String> families = new HashSet<String>(changed.keySet());
		families.addAll(removed.keySet());
		families.addAll(increments.keySet());
		if (families.isEmpty())
			families.add(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME);
		HTable t = this.getTable(table, families.toArray(new String[families.size()]));

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

		if (rowPut == null && (increments == null || increments.isEmpty())) { //NOT rowDel == null; deleting an element that becomes empty actually deletes the element !
			rowPut = new Put(row);
			rowPut.add(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME), null, new byte[]{});
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
		
		if (problem instanceof RetriesExhaustedException) {
			RetriesExhaustedException re = (RetriesExhaustedException)problem;
			String message = re.getMessage();
			if (message.contains("TableNotFoundException")) {
				uncache(t);
				this.storeChanges(table, id, changed, removed, increments);
			}
		} else if (problem instanceof TableNotFoundException) {
			uncache(t);
			this.storeChanges(table, id, changed, removed, increments);
		} else if (problem != null)
			throw new DatabaseNotReachedException(problem);
	}

	@Override
	public void delete(String table, String id)
			throws DatabaseNotReachedException {
		HTable t = this.getTable(table);

		Delete rowDel = new Delete(Bytes.toBytes(id));
		try {
			t.delete(rowDel);
		} catch (IOException e) {
			throw new DatabaseNotReachedException(e);
		}
	}

	protected long count(String table, Scan s) throws DatabaseNotReachedException {
		return this.countSimple(table, s);
	}

	protected long countSimple(String table, Scan s) throws DatabaseNotReachedException {
		if (! this.hasTableNoCache(table))
			return 0;
		
		HTable t = this.getTable(table);
		ResultScanner r = null;
		try {
			int count = 0;
			r = t.getScanner(s);
			Iterator<Result> it = r.iterator();
			while (it.hasNext()) {
				it.next();
				count++;
			}
			return count;
		} catch (IOException e) {
			throw new DatabaseNotReachedException(e);
		} finally {
			if (r != null)
				r.close();
		}
	}
	
	protected long countMapRed(String table, Scan s) throws DatabaseNotReachedException {
		if (!this.hasTableNoCache(table))
			return 0;

		String tableName = this.mangleTableName(table);
		try {
			Job count = RowCounter.createSubmittableJob(getConf(), tableName, s);
			if(!count.waitForCompletion(false))
				throw new DatabaseNotReachedException("Row count failed for table " + table);
			return count.getCounters().findCounter(RowCounter.RowCounterMapper.Counters.ROWS).getValue();
		} catch (IOException e) {
			throw new DatabaseNotReachedException(e);
		} catch (InterruptedException e) {
			throw new DatabaseNotReachedException(e);
		} catch (ClassNotFoundException e) {
			throw new DatabaseNotReachedException(e);
		}
	}

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
		if (!this.hasColumnFamily(table, family))
			return false;

		HTable t = this.getTable(table);
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

		HTable t = this.getTable(table);
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
		HTable t = this.getTable(table, family);
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
		HTable t = this.getTable(table, family);

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

	protected Scan getScan(Constraint c, String... families) throws DatabaseNotReachedException {
		Scan s = new Scan();
		if (c != null && c.getStartKey() != null)
			s.setStartRow(Bytes.toBytes(c.getStartKey()));
		if (c != null && c.getEndKey() != null) {
			byte[] endb = Bytes.toBytes(c.getEndKey());
			endb = Bytes.add(endb, new byte[] { 0 });
			s.setStopRow(endb);
		}
		
		if (families != null) {
			for (String fam : families) {
				s.addFamily(Bytes.toBytes(fam));
			}
		} else {
			//No family to load ; avoid getting all information in the row (that may be big)
			s.setFilter(new FirstKeyOnlyFilter());
		}
		
		return s;
	}

	@Override
	public long count(String table, Constraint c) throws DatabaseNotReachedException {
		return this.count(table, this.getScan(c));
	}

	@Override
	public com.googlecode.n_orm.storeapi.CloseableKeyIterator get(String table, Constraint c,
			 int limit, Set<String> families) throws DatabaseNotReachedException {
		if (!this.hasTable(table))
			return new EmptyIterator();

		HTable ht = this.getTable(table);
		
		Scan s = this.getScan(c, families == null ? null : families.toArray(new String[families.size()]));
		Filter filter = s.getFilter();
		FilterList fl;
		if (filter instanceof FilterList) {
			fl = (FilterList)filter;
			fl.addFilter(new PageFilter(limit));
		} else {
			s.setFilter(new PageFilter(limit));
		}
		
		final ResultScanner r;
		try {
			r = ht.getScanner(s);
		} catch (TableNotFoundException x) {
			uncache(ht);
			return new EmptyIterator();
		} catch (IOException e) {
			throw new DatabaseNotReachedException(e);
		}
		return new CloseableIterator(r, families != null);
	}

	public void truncate(String table, Constraint c) throws DatabaseNotReachedException {
		this.truncate(table, this.getScan(c));
	}
	
	protected void truncate(String table, Scan s) {
		this.truncateSimple(table, s);
	}
	
	protected void truncateSimple(String table, Scan s)  {
		if (! this.hasTableNoCache(table))
			return;
		
		HTable t = this.getTable(table);
		ResultScanner r = null;
		try {
			r = t.getScanner(s);
			Iterator<Result> it = r.iterator();
			while (it.hasNext()) {
				t.delete(new Delete(it.next().getRow()));
			}
		} catch (IOException e) {
			throw new DatabaseNotReachedException(e);
		} finally {
			if (r != null)
				r.close();
		}
	}
	
	protected void truncateMapReduce(String table, Scan s)  {
		if (!this.hasTableNoCache(table))
			return;

		String tableName = this.mangleTableName(table);
		try {
			Job count = Truncator.createSubmittableJob(getConf(), tableName, s);
			if(!count.waitForCompletion(false))
				throw new DatabaseNotReachedException("Truncate failed for table " + table);
		} catch (IOException e) {
			throw new DatabaseNotReachedException(e);
		} catch (InterruptedException e) {
			throw new DatabaseNotReachedException(e);
		} catch (ClassNotFoundException e) {
			throw new DatabaseNotReachedException(e);
		}
	}

}
