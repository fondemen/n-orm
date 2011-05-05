package com.googlecode.n_orm.hbase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

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
import com.googlecode.n_orm.EmptyCloseableIterator;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.hbase.RecursiveFileAction.Report;
import com.googlecode.n_orm.hbase.actions.Action;
import com.googlecode.n_orm.hbase.actions.BatchAction;
import com.googlecode.n_orm.hbase.actions.DeleteAction;
import com.googlecode.n_orm.hbase.actions.ExistsAction;
import com.googlecode.n_orm.hbase.actions.GetAction;
import com.googlecode.n_orm.hbase.actions.ScanAction;
import com.googlecode.n_orm.hbase.mapreduce.RowCounter;
import com.googlecode.n_orm.hbase.mapreduce.Truncator;
import com.googlecode.n_orm.storeapi.Constraint;

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

	public static final Logger logger;
	public static final Logger errorLogger;
	
	protected static Map<Properties, Store> knownStores = new HashMap<Properties, Store>();
	
	static {
		logger = Logger.getLogger(Store.class.getName());
		errorLogger = Logger.getLogger(Store.class.getName());
		initSimpleLogger(logger, System.out);
		initSimpleLogger(errorLogger, System.err);
	}
	
	private static void initSimpleLogger(Logger logger, PrintStream out) {
		StreamHandler handler = new StreamHandler(System.out, new SimpleFormatter());
		logger.addHandler(handler);
		for (Handler h : logger.getHandlers()) {
			if (h != handler)
				logger.removeHandler(h);
		}
	}

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
				logger.info("Creating store for " + host + ':' + port);
				ret = new Store(p);
				ret.setHost(host);
				ret.setPort(port);
				if (maxRetries != null)
					ret.setMaxRetries(maxRetries);
				knownStores.put(p, ret);
				logger.info("Created store " + ret.hashCode() + " for " + host + ':' + port);
			}
			return ret;
		}
	}
	
	/**
	 * Get an HBase store according to a set of comma-separated configuration folders.
	 * Those folders are supposed to have configuration files following the pattern *-site.xml. 
	 */
	public static Store getStore(String commaSeparatedConfigurationFolders) throws IOException {
		synchronized(Store.class) {
			Properties p = new Properties();
			p.setProperty("commaSeparatedConfigurationFolders", commaSeparatedConfigurationFolders);
			Store ret = knownStores.get(p);
			
			if (ret == null) {
				logger.info("Creating store for " + commaSeparatedConfigurationFolders);
				Configuration conf = new Configuration();
				ReportConf r = new ReportConf(conf);
				for (String  configurationFolder : commaSeparatedConfigurationFolders.split(",")) {
					
					File confd = new File(configurationFolder);
					if (!confd.isDirectory()) {
						errorLogger.config("Cannot read HBase configuration folder " + confd);
						continue;
					}
					
					addConfAction.recursiveManageFile(confd, r);
				}
			
				if (!r.foundPropertyFile)
					throw new IOException("No configuration file found in the following folders " + commaSeparatedConfigurationFolders + " ; expecting some *-site.xml files");
				if (!r.foundHBasePropertyFile)
					throw new IOException("Could not find hbase-site.xml from folders " + commaSeparatedConfigurationFolders);
				
				ret = new Store(p);
				ret.setConf(HBaseConfiguration.create(conf));
				
				knownStores.put(p, ret);
				logger.info("Created store " + ret.hashCode() + " for " + commaSeparatedConfigurationFolders);
			}
			
			return ret;
		}
	}
	
	public static Store getKnownStore(Properties properties) {
		return knownStores.get(properties);
	}
	
	private final Properties launchProps;
	private String host = "localhost";
	private int port = HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;
	private Integer maxRetries = null;
	private boolean wasStarted = false;
	private Configuration config;
	private HBaseAdmin admin;
	public Map<String, HTableDescriptor> tablesD = new TreeMap<String, HTableDescriptor>();
	public Map<String, HTable> tablesC = new TreeMap<String, HTable>();

	protected Store(Properties properties) {
		this.launchProps = properties;
	}

	public synchronized void start() throws DatabaseNotReachedException {
		if (this.wasStarted)
			return;
		
		logger.info("Starting store " + this.hashCode());

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
				logger.fine("Connecting HBase admin for store " + this.hashCode());
				this.setAdmin(new HBaseAdmin(this.config));
				logger.fine("Connected HBase admin for store " + this.hashCode());
				if (!this.admin.isMasterRunning()) {
					errorLogger.severe("No HBase master running for store " + this.hashCode());
					throw new DatabaseNotReachedException(new MasterNotRunningException());
				}
			} catch (Exception e) {
				errorLogger.severe("Could not cpnnect HBase for store " + this.hashCode() + " (" +e.getMessage() +')');
				throw new DatabaseNotReachedException(e);
			}
			
		logger.info("Started store " + this.hashCode());

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

	public Properties getLaunchProps() {
		return launchProps;
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

	protected synchronized void restart() {
		synchronized(this.tablesD) {
			this.tablesD.clear();
			synchronized(this.tablesC) {
				this.tablesC.clear();
			}
		}
		this.config = HBaseConfiguration.create(this.config);
		this.admin = null;
		this.wasStarted = false;
		this.start();
	}
	
	private void handleProblem(Exception problem, String table) throws DatabaseNotReachedException {
		errorLogger.log(Level.FINE, "Trying to recover from exception for store " + this.hashCode() + " it seems that a table was dropped", problem);
		if (problem instanceof DatabaseNotReachedException)
			throw (DatabaseNotReachedException)problem;
		
		if (problem instanceof TableNotFoundException) {
			table = this.mangleTableName(table);
			errorLogger.log(Level.INFO, "Trying to recover from exception for store " + this.hashCode() + " it seems that a table was dropped ; restarting store", problem);
			if (this.tablesD.containsKey(table))
				this.uncache(table);
			else
				throw new DatabaseNotReachedException(problem);
		} else if (problem instanceof IOException) {
			if (problem.getMessage().contains("closed")) {
				errorLogger.log(Level.INFO, "Trying to recover from exception for store " + this.hashCode() + " it seems that connection was lost ; restarting store", problem);
				restart();
				return;
			}
			
			throw new DatabaseNotReachedException(problem);
		}
	}
	
	private HTable handleProblemAndGetTable(Exception problem, String table, String... families) throws DatabaseNotReachedException {
		this.handleProblem(problem, table);
		return this.getTable(table, families);
	}
	
	protected <R> R tryPerform(Action<R> action, HTable table, String tableName, String... expectedFamilies) throws DatabaseNotReachedException {
		if (table == null)
			table = this.getTable(tableName, expectedFamilies);
		action.setTable(table);
		try {
			return action.perform();
		} catch (Exception e) {
			errorLogger.log(Level.INFO, "Got an error while performing a " + action.getClass().getName() + " on table " + Bytes.toString(action.getTable().getTableName()) + " for store " + this.hashCode(), e);
			table = this.handleProblemAndGetTable(e, tableName, expectedFamilies);
			action.setTable(table);
			try {
				errorLogger.log(Level.INFO, "Retrying to perform again erroneous " + action.getClass().getName() + " on table " + Bytes.toString(action.getTable().getTableName()) + " for store " + this.hashCode(), e);
				return action.perform();
			} catch (Exception f) {
				errorLogger.log(Level.SEVERE, "Cannot recover from error while performing a" + action.getClass().getName() + " on table " + Bytes.toString(action.getTable().getTableName()) + " for store " + this.hashCode(), e);
				throw new DatabaseNotReachedException(f);
			}
		}
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
				logger.fine("Unknown table " + name + " for store " + this.hashCode());
				try {
					if (!this.admin.tableExists(name)) {
						logger.info("Table " + name + " not found ; creating with column families " + Arrays.toString(expectedFamilies));
						td = new HTableDescriptor(name);
						if (expectedFamilies != null) {
							for (String fam : expectedFamilies) {
								td.addFamily(new HColumnDescriptor(fam));
							}
						}
						this.admin.createTable(td);
						logger.info("Table " + name + " created with column families " + Arrays.toString(expectedFamilies));
						created = true;
					} else {
						td = this.admin.getTableDescriptor(Bytes.toBytes(name));
						logger.fine("Got descriptor for table " + name);
					}
					this.cache(name, td);
				} catch (IOException e) {
					throw new DatabaseNotReachedException(e);
				}
			} else {
				td = this.tablesD.get(name);
			}
		}

		if (!created && expectedFamilies != null && expectedFamilies.length>0) {
			this.enforceColumnFamiliesExists(td, expectedFamilies);
		}

		synchronized (this.tablesC) {
			HTable ret = this.tablesC.get(name);
			if (ret == null) {
				try {
					logger.fine("Creating accessor for table " + name);
					ret = new HTable(this.config, name);
					this.cache(name, ret);
					logger.fine("Created accessor for table " + name);
					return ret;
				} catch (IOException e) {
					errorLogger.log(Level.SEVERE, "Could not create accessor for table " + name, e);
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
		List<HColumnDescriptor> toBeAdded = new ArrayList<HColumnDescriptor>(columnFamilies.length);
		synchronized (tableD) {
			boolean recreated = false;
			for (String cf : columnFamilies) {
				byte[] cfname = Bytes.toBytes(cf);
				if (!recreated && !tableD.hasFamily(cfname)) {
					String tableName = tableD.getNameAsString();
					logger.fine("Table " + tableName + " is not known to have family " + cf + ": checking from HBase");
					try {
						tableD = this.admin.getTableDescriptor(tableD.getName());
					} catch (Exception e) {
						errorLogger.log(Level.INFO, " Problem while getting descriptor for " + tableName + "; retrying", e);
						this.handleProblemAndGetTable(e, tableName, columnFamilies);
						return;
					}
					this.cache(tableName, tableD);
					recreated = true;
				}
				if (!tableD.hasFamily(cfname)) {
					HColumnDescriptor newFamily = new HColumnDescriptor(cfname);
					toBeAdded.add(newFamily);
					tableD.addFamily(newFamily);
				}
			}
			if (!toBeAdded.isEmpty()) {
				try {
					logger.info("Table " + tableD.getNameAsString() + " does have families " + toBeAdded.toString() + ": creating");
					this.admin.disableTable(tableD.getName());
					for (HColumnDescriptor hColumnDescriptor : toBeAdded) {
						this.admin.addColumn(tableD.getName(),hColumnDescriptor);
					}
					this.admin.enableTable(tableD.getName());
					logger.info("Table " + tableD.getNameAsString() + " does have families " + toBeAdded.toString() + ": created");
				} catch (IOException e) {
					errorLogger.log(Level.SEVERE, "Could not create on table " + tableD.getNameAsString() + " families " + toBeAdded.toString(), e);
					throw new DatabaseNotReachedException(e);
				}

			}
		}
	}

	@Override
	public Map<String, Map<String, byte[]>> get(String table, String id,
			Set<String> families) throws DatabaseNotReachedException {
		Get g = new Get(Bytes.toBytes(id));
		for (String family : families) {
			g.addFamily(Bytes.toBytes(family));
		}

		Result r = this.tryPerform(new GetAction(g), null, table, families.toArray(new String[families.size()]));
		
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
		
		String[] famAr = families.toArray(new String[families.size()]);
		HTable t = this.getTable(table, famAr);

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
		
		List<org.apache.hadoop.hbase.client.Row> actions = new ArrayList<org.apache.hadoop.hbase.client.Row>(2);
		if (rowPut != null) actions.add(rowPut);
		if (rowDel != null) actions.add(rowDel);
		
		if (! actions.isEmpty()) {
			BatchAction act = new BatchAction(actions);
			this.tryPerform(act, t, table, famAr);
			t = act.getTable();
		}

		int maxTries = 3;
		boolean done = false;
		do {
			maxTries--;
			try {
				for (String fam : increments.keySet()) {
					Map<String, Number> incrs = increments.get(fam);
					Iterator<Entry<String, Number>> incri = incrs.entrySet().iterator();
					while (incri.hasNext()) {
						Entry<String, Number> incr = incri.next();
						t.incrementColumnValue(row, Bytes.toBytes(fam),
								Bytes.toBytes(incr.getKey()), incr.getValue().longValue());
						incri.remove();
					}
				}
				done = true;
			} catch (IOException e) {
				if (maxTries > 0) {
					t = this.handleProblemAndGetTable(e, table, famAr);
				} else
					throw new DatabaseNotReachedException(e);
			}
		} while (!done);
	}

	@Override
	public void delete(String table, String id)
			throws DatabaseNotReachedException {
		Delete rowDel = new Delete(Bytes.toBytes(id));
		this.tryPerform(new DeleteAction(rowDel), null, table);
	}

	protected long count(String table, Scan s) throws DatabaseNotReachedException {
		return this.countSimple(table, s);
		//return this.countMapRed(table, s);
	}

	protected long countSimple(String table, Scan s) throws DatabaseNotReachedException {
		if (! this.hasTableNoCache(table))
			return 0;
		
		ResultScanner r = this.tryPerform(new ScanAction(s), null, table);
		int count = 0;
		try {
			Iterator<Result> it = r.iterator();
			while (it.hasNext()) {
				it.next();
				count++;
			}
			return count;
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
			Job count = RowCounter.createSubmittableJob(this, tableName, s);
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

	@Override
	public boolean exists(String table, String row, String family)
			throws DatabaseNotReachedException {
		if (!this.hasColumnFamily(table, family))
			return false;

		Get g = new Get(Bytes.toBytes(row)).addFamily(Bytes.toBytes(family));
		g.setFilter(new FirstKeyOnlyFilter());
		return this.tryPerform(new ExistsAction(g), null, table);
	}

	@Override
	public boolean exists(String table, String row)
			throws DatabaseNotReachedException {
		if (!this.hasTable(table))
			return false;

		Get g = new Get(Bytes.toBytes(row));
		g.setFilter(new FirstKeyOnlyFilter());
		return this.tryPerform(new ExistsAction(g), null, table);
	}

	@Override
	public byte[] get(String table, String row, String family, String key)
			throws DatabaseNotReachedException {
		Get g = new Get(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family),
				Bytes.toBytes(key));

		Result result = this.tryPerform(new GetAction(g), null, table, family);
		
		if (result.isEmpty())
			return null;
		return result.value();
	}

	@Override
	public Map<String, byte[]> get(String table, String id, String family)
			throws DatabaseNotReachedException {
		return this.get(table, id, family, (Constraint) null);
	}

	@Override
	public Map<String, byte[]> get(String table, String id, String family,
			Constraint c) throws DatabaseNotReachedException {
		Get g = new Get(Bytes.toBytes(id)).addFamily(Bytes.toBytes(family));

		if (c != null) {
			g.setFilter(createFamilyConstraint(c, false));
		}

		Result r = this.tryPerform(new GetAction(g), null, table, family);
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
			return new EmptyCloseableIterator();
		
		String[] famAr = families == null ? null : families.toArray(new String[families.size()]);
		Scan s = this.getScan(c, famAr);
		Filter filter = s.getFilter();
		FilterList fl;
		if (filter instanceof FilterList) {
			fl = (FilterList)filter;
			fl.addFilter(new PageFilter(limit));
		} else {
			s.setFilter(new PageFilter(limit));
		}
		
		ResultScanner r = this.tryPerform(new ScanAction(s), null, table, famAr);
		return new CloseableIterator(r, families != null);
	}

	public void truncate(String table, Constraint c) throws DatabaseNotReachedException {
		this.truncate(table, this.getScan(c));
	}
	
	protected void truncate(String table, Scan s) {
		//this.truncateMapReduce(table, s);
		this.truncateSimple(table, s);
	}
	
	protected void truncateSimple(String table, Scan s)  {
		if (! this.hasTableNoCache(table))
			return;
		
		logger.info("Truncating table " + table);
		
		HTable t = this.getTable(table);
		ScanAction action = new ScanAction(s);
		ResultScanner r = this.tryPerform(action, t, table);
		t = action.getTable();

		try {
			final int nbRows = 100;
			List<Delete> dels = new ArrayList<Delete>(nbRows);
			Result [] res = r.next(nbRows);
			while (res != null && res.length != 0) {
				dels.clear();
				for (Result result : res) {
					dels.add(new Delete(result.getRow()));
				}
				t.delete(dels);
				res = r.next(nbRows);
			}
			logger.info("Truncated table " + table);
		} catch (IOException e) {
			errorLogger.log(Level.INFO, "Could not truncate table " + table, e);
			throw new DatabaseNotReachedException(e);
		} finally {
			if (r != null)
				r.close();
		}
	}
	
	protected void truncateMapReduce(String table, Scan s)  {
		if (!this.hasTableNoCache(table))
			return;
		
		logger.info("Truncating table " + table + " using map/reduce job");

		String tableName = this.mangleTableName(table);
		try {
			Job count = Truncator.createSubmittableJob(this, tableName, s);
			if(!count.waitForCompletion(false)) {
				errorLogger.info("Could not truncate table with map/reduce " + table);
				throw new DatabaseNotReachedException("Truncate failed for table " + table);
			}
			logger.info("Truncated table " + table + " using map/reduce job");
		} catch (IOException e) {
			errorLogger.log(Level.INFO, "Could not truncate table " + table, e);
			throw new DatabaseNotReachedException(e);
		} catch (InterruptedException e) {
			errorLogger.log(Level.INFO, "Could not truncate table " + table, e);
			throw new DatabaseNotReachedException(e);
		} catch (ClassNotFoundException e) {
			errorLogger.log(Level.INFO, "Could not truncate table " + table, e);
			throw new DatabaseNotReachedException(e);
		}
	}

}
