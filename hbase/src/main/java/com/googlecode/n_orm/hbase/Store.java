package com.googlecode.n_orm.hbase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
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
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerTimeoutException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.io.SetFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.recipes.lock.SharedExclusiveLock;
import org.codehaus.plexus.util.DirectoryScanner;

import com.googlecode.n_orm.Callback;
import com.googlecode.n_orm.ColumnFamiliyManagement;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.EmptyCloseableIterator;
import com.googlecode.n_orm.Incrementing;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.Process;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.cache.Cache;
import com.googlecode.n_orm.hbase.RecursiveFileAction.Report;
import com.googlecode.n_orm.hbase.actions.Action;
import com.googlecode.n_orm.hbase.actions.BatchAction;
import com.googlecode.n_orm.hbase.actions.CountAction;
import com.googlecode.n_orm.hbase.actions.DeleteAction;
import com.googlecode.n_orm.hbase.actions.ExistsAction;
import com.googlecode.n_orm.hbase.actions.GetAction;
import com.googlecode.n_orm.hbase.actions.IncrementAction;
import com.googlecode.n_orm.hbase.actions.PutAction;
import com.googlecode.n_orm.hbase.actions.ScanAction;
import com.googlecode.n_orm.hbase.actions.TruncateAction;
import com.googlecode.n_orm.hbase.mapreduce.ActionJob;
import com.googlecode.n_orm.hbase.mapreduce.LocalFormat;
import com.googlecode.n_orm.hbase.mapreduce.RowCounter;
import com.googlecode.n_orm.hbase.mapreduce.Truncator;
import com.googlecode.n_orm.query.SearchableClassConstraintBuilder;
import com.googlecode.n_orm.storeapi.ActionnableStore;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.Row;
import com.googlecode.n_orm.storeapi.TypeAwareStoreWrapper;

/**
 * The HBase store found according to its configuration folder.
 * An example store.properties file is:<br><code>
 * class=com.googlecode.n_orm.hbase.Store<br>
 * static-accessor=getStore<br>
 * 1=/usr/lib/hbase/conf/
 * </code><br>
 * Given files are explored recursively ignoring files given with a ! prefix. You can also define  (positive or negative with a ! prefix) filters using wilcards such as * (any character set), ? (any character), and ** (any sub-directory) can be used both in included and excluded patterns (see {@link DirectoryScanner}), but at least one directory to look in must be defined without wildcard.
 * Two attempts are performed during search: first explicitly looking for ./*-site.xml and ./conf/*-site.xml, and then all possible ** /*-site.xml. hbase-site.xml MUST be found for the operation to succeed.
 * Compared to {@link HBase}, no jar found in those is added to classpath.
 * For test purpose, you can also directly reach an HBase instance thanks to one of its zookeeper host and client port:<br><code>
 * class=com.googlecode.n_orm.hbase.Store<br>
 * static-accessor=getStore<br>
 * 1=localhost<br>
 * 2=2181
 * compression=gz &#35;can be 'none', 'gz', 'lzo', or 'snappy' (default is 'none') ; in the latter two cases, take great care that those compressors are available for all nodes of your hbase cluster
 * </code><br>
 * One important property to configure is {@link #setScanCaching(Integer)}.<br>
 * This store supports remote processes (see {@link StorageManagement#processElementsRemotely(Class, Constraint, Process, Callback, int, String...)} and {@link SearchableClassConstraintBuilder#remoteForEach(Process, Callback)}) as it implements {@link ActionnableStore} by using HBase/Hadoop Map-only jobs. However, be careful when configuring your hadoop: all jars containing your process and n-orm (with dependencies) should be available.
 * By default, all known jars are sent (which might become a problem is same jars are sent over and over).
 * You can change this using e.g. {@link #setMapRedSendJars(boolean)}.
 */
public class Store /*extends TypeAwareStoreWrapper*/ implements com.googlecode.n_orm.storeapi.GenericStore, ActionnableStore {

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

	public static final Logger logger = HBase.logger;
	public static final Logger errorLogger = HBase.errorLogger;
	public static final String localHostName;
	public static final long lockTimeout = 10000;
	private static List<String> unavailableCompressors = new ArrayList<String>();
	
	protected static Map<Properties, Store> knownStores = new HashMap<Properties, Store>();
	
	static {
		String lhn;
		try {
			lhn = java.net.InetAddress.getLocalHost().getHostName();
		} catch (Exception x) {
			lhn = "localhost";
			errorLogger.log(Level.WARNING, "Cannot get local host name", x);
		}
		localHostName = lhn;
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
				
				//First attempt using usual configuration
				String cscf = commaSeparatedConfigurationFolders + ",conf/*-site.xml,*-site.xml,!**/*example*/**,!**/*src*/**";
				addConfAction.clear();
				addConfAction.addFiles(cscf);
				try {
					addConfAction.explore(r);
				} catch (IllegalArgumentException x) {
					throw new DatabaseNotReachedException("Invalid configuration folders specification " + commaSeparatedConfigurationFolders + ": " + x.getMessage());
				}
				if (!r.foundPropertyFile || !r.foundHBasePropertyFile) {
					//Second attempt exploring all possibilities
					cscf = commaSeparatedConfigurationFolders;
					addConfAction.clear();
					addConfAction.addFiles(cscf);
					addConfAction.explore(r);
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
	
	//For inter-processes synchronization:
	//avoids different processes to alter schema concurrently
	private Map<String /*tableName*/ , SharedExclusiveLock /*lock*/> locks = new TreeMap<String, SharedExclusiveLock>();
	//Those tables that are locked in exclusive mode and should be back to shared mode
	private Set<String /*tableName*/> sharedExclusiveLockedTables = new TreeSet<String>();
	
	private String host = localHostName;
	private int port = HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;
	private Integer maxRetries = null;
	private boolean wasStarted = false;
	private volatile boolean restarting = false;
	private final Object restartMutex = new Object();
	private Configuration config;
	private HBaseAdmin admin;
	public Map<String, HTableDescriptor> tablesD = new TreeMap<String, HTableDescriptor>();
	public HTablePool tablesC;
	
	private Integer scanCaching = null;
	private Algorithm compression;
	private boolean forceCompression = false;
	
	private boolean countMapRed = false;
	private boolean truncateMapRed = false;
	
	private int mapRedScanCaching = 500;
	private boolean mapRedSendHBaseJars = true;
	private boolean mapRedSendNOrmJars = true;
	private boolean mapRedSendJobJars = true;

	protected Store(Properties properties) {
		this.launchProps = properties;
		try {
			host = java.net.InetAddress.getLocalHost().getHostName();
		} catch (Exception x) {
			errorLogger.log(Level.WARNING, "Cannot get local host name", x);
		}
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
		
		this.tablesC = new HTablePool(this.getConf(), Integer.MAX_VALUE);
		
		//Wait fo Zookeeper availability
		int maxRetries = 100;
		ZooKeeper zk = null;
		do {
			try {
				zk = this.admin.getConnection().getZooKeeperWatcher().getZooKeeper();
			} catch (Exception x) {
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
				}
			}
		} while (zk == null && maxRetries-- > 0);
		if (zk == null) {
			logger.log(Level.SEVERE, "Cannot reach Zookeeper");
		}
		
		try {
			String[] host = this.admin.getConnection().getZooKeeperWatcher().getQuorum().split(",")[0].split(":");
			this.host = host[0].trim();
			if (host.length > 1)
				this.port = Integer.parseInt(host[1].trim());
			else
				this.port = HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;
		} catch (Exception e) {
			errorLogger.log(Level.WARNING, "Cannot read zookeeper info... Might be a bug.", e);
		}
			
		logger.info("Started store " + this.hashCode());

		this.wasStarted = true;
	}

	/**
	 * The zookeeper host to be used.
	 * You can only trust this method is this store was explicitly set the host before or started.
	 */
	public String getHost() {
		return host;
	}

	/**
	 * The zookeeper quorum host to be used.
	 * You shouldn't use this method as it should be set by {@link #getStore(String)}, {@link #getStore(String, int)}, or {@link #getStore(String, int, Integer)}.
	 */
	@Override
	public void setHost(String url) {
		this.host = url;
	}

	/**
	 * The zookeeper quorum port to be used.
	 * You can only trust this method is this store was explicitly set the port before or started.
	 */
	public int getPort() {
		return port;
	}

	/**
	 * The zookeeper quorum port to be used.
	 * You shouldn't use this method as it should be set by {@link #getStore(String)}, {@link #getStore(String, int)}, or {@link #getStore(String, int, Integer)}.
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * The number of times this store can retry connecting the cluster.
	 * Default value is HBase default value (10).
	 * @throws IllegalArgumentException in case the sent value is less that 1
	 */
	public void setMaxRetries(int maxRetries) {
		if (maxRetries <= 0)
			throw new IllegalArgumentException("Cannot retry less than once");
		this.maxRetries = Integer.valueOf(maxRetries);
	}

	/**
	 * The number of elements that this store scans at once during a search.
	 * @return the expected value, or null if not set (equivalent to the HBase default value - 1 - see <a href="http://hbase.apache.org/book/perf.reading.html#perf.hbase.client.caching">the HBase documentation</a>)
	 */
	public Integer getScanCaching() {
		return scanCaching;
	}

	/**
	 * The number of elements that this store scans at once during a search.
	 * Default value is HBase default value (1).
	 * Use this carefully ; read <a href="http://hbase.apache.org/book/perf.reading.html#perf.hbase.client.caching">the HBase documentation</a>.
	 */
	public void setScanCaching(Integer scanCaching) {
		this.scanCaching = scanCaching;
	}

	/**
	 * The number of elements that this store scans at once during a Map/Reduce task (see <a href="http://hbase.apache.org/book/perf.reading.html#perf.hbase.client.caching">the HBase documentation</a>).
	 * @see StorageManagement#processElementsRemotely(Class, Constraint, Process, Callback, int, String...)
	 * @see SearchableClassConstraintBuilder#remoteForEach(Process, Callback)
	 */
	public int getMapRedScanCaching() {
		return mapRedScanCaching;
	}

	public void setMapRedScanCaching(int mapRedScanCaching) {
		this.mapRedScanCaching = mapRedScanCaching;
	}

	/**
	 * The used compression for this store.
	 * Compression is used when creating a column family in the HBase cluster.
	 * In case you set {@link #setForceCompression(boolean)} to true, existing column families are also checked and altered if necessary.
	 * @return the used compression, or null if not set (equivalent to the HBase default value - none)
	 */
	public String getCompression() {
		if (compression == null)
			return null;
		return compression.getName();
	}

	/**
	 * The used compression for this store.
	 * Default value is the HBase default (none).
	 * Compression is used when creating a column family in the HBase cluster.
	 * In case you set {@link #setForceCompression(boolean)} to true, existing column families are also checked and altered if necessary.
	 */
	public void setCompression(String compression) {
		if (compression == null) {
			this.compression = null;
		} else {
			for (String cmp : compression.split("-or-")) {
				if (unavailableCompressors.contains(cmp))
					continue;
				try {
					this.compression = Compression.getCompressionAlgorithmByName(cmp);
					break;
				} catch (Exception x) {
					unavailableCompressors.add(cmp);
					logger.warning("Could not use compression " + cmp);
				}
			}
		}
	}

	/**
	 * Whether existing columns have to be altered if they don't use the correct compressor.
	 * see {@link #getCompression()}
	 */
	public boolean isForceCompression() {
		return forceCompression;
	}


	/**
	 * Whether existing columns have to be altered if they don't use the correct compressor.
	 * Default value is false.
	 * Be careful with this parameter as if two process have a store on the same cluster each with {@link #setForceCompression(boolean)} to true and different values for {@link Store#setCompression(String)} : column families might be altered in an endless loop !
	 * Note that altering a column family takes some time as tables must be disabled and enabled again, so use this with care.
	 * see {@link #getCompression()}
	 */
	public void setForceCompression(boolean forceCompression) {
		this.forceCompression = forceCompression;
	}

	/**
	 * Whether counts (e.g. {@link #count(String, Constraint)}) should use a map/reduce job.
	 */
	public boolean isCountMapRed() {
		return countMapRed;
	}

	/**
	 * Whether counts (e.g. {@link Store#count(String, Constraint)}) should use a map/reduce job.
	 * Default value is false.
	 * Map/reduce jobs are usually hard to run, so if this method is faster in case of large data on large cluster, it should be avoided on small clusters.
	 */
	public void setCountMapRed(boolean countMapRed) {
		this.countMapRed = countMapRed;
	}

	/**
	 * Whether truncates (e.g. {@link #truncate(String, Constraint)}) should use a map/reduce job.
	 */
	public boolean isTruncateMapRed() {
		return truncateMapRed;
	}

	/**
	 * Whether truncates (e.g. {@link #truncate(String, Constraint)}) should use a map/reduce job.
	 * Default value is false.
	 * Map/reduce jobs are usually hard to run, so if this method is faster in case of large data on large cluster, it should be avoided on small clusters.
	 */
	public void setTruncateMapRed(boolean truncateMapRed) {
		this.truncateMapRed = truncateMapRed;
	}

	/**
	 * Whether jar files containing sent jobs should be sent to the Hadoop Map/Reduce cluster while performing a Map/Reduce job.
	 */
	public boolean isMapRedSendJobJars() {
		return mapRedSendJobJars;
	}

	/**
	 * Whether jar files containing sent jobs should be sent to the Hadoop Map/Reduce cluster while performing a Map/Reduce job.
	 * Default value is true.
	 * Setting this parameter to false will improve map/reduce tasks setup, but you might face {@link ClassNotFoundException} on task tracker nodes if their CLASSPATH is not configured properly.
	 * To be used with care !
	 */
	public void setMapRedSendJobJars(boolean mapRedSendJars) {
		this.mapRedSendJobJars = mapRedSendJars;
	}

	/**
	 * Whether jar files containing n-orm and the n-orm HBase driver should be sent to the Hadoop Map/Reduce cluster while performing a Map/Reduce job.
	 */
	public boolean isMapRedSendNOrmJars() {
		return mapRedSendNOrmJars;
	}

	/**
	 * Whether jar files containing n-orm and the n-orm HBase driver should be sent to the Hadoop Map/Reduce cluster while performing a Map/Reduce job.
	 * Default value is true.
	 * Setting this parameter to false will improve map/reduce tasks setup, but you might face {@link ClassNotFoundException} on task tracker nodes if their CLASSPATH is not configured properly.
	 * To be used with care !
	 */
	public void setMapRedSendNOrmJars(boolean mapRedSendJars) {
		this.mapRedSendNOrmJars = mapRedSendJars;
	}

	/**
	 * Whether jar files containing the HBase client should be sent to the Hadoop Map/Reduce cluster while performing a Map/Reduce job.
	 */
	public boolean isMapRedSendHBaseJars() {
		return mapRedSendHBaseJars;
	}

	/**
	 * Whether jar files containing the HBase client should be sent to the Hadoop Map/Reduce cluster while performing a Map/Reduce job.
	 * Default value is true.
	 * Setting this parameter to false will improve map/reduce tasks setup, but you might face {@link ClassNotFoundException} on task tracker nodes if their CLASSPATH is not configured properly.
	 * To be used with care !
	 */
	public void setMapRedSendHBaseJars(boolean mapRedSendJars) {
		this.mapRedSendHBaseJars = mapRedSendJars;
	}

	/**
	 * Whether jar files containing HBase, n-orm, the n-orm HBase driver and sent job should be sent to the Hadoop Map/Reduce cluster while performing a Map/Reduce job.
	 * Default value is true.
	 * Setting this parameter to false will improve map/reduce tasks setup, but you might face {@link ClassNotFoundException} on task tracker nodes if their CLASSPATH is not configured properly.
	 * To be used with care !
	 */
	public void setMapRedSendJars(boolean mapRedSendJars) {
		this.setMapRedSendJobJars(mapRedSendJars);
		this.setMapRedSendNOrmJars(mapRedSendJars);
		this.setMapRedSendHBaseJars(mapRedSendJars);
	}

	/**
	 * The configuration used by this store.
	 * You can only trust this method is this store was explicitly set the host before or started.
	 * This method provides a mean to have greater control over HBase and Hadoop.
	 */
	public Configuration getConf() {
		return this.config;
	}

	public Properties getLaunchProps() {
		return launchProps;
	}


	/**
	 * The configuration to be used by this store for its {@link #start()} or {@link #restart()}.
	 * Only valid when store is not started yet.
	 * Overloads any other configuration setting already set by {@link #getStore(String)}, {@link #getStore(String, int)}, {@link #getStore(String, int, Integer)}, or {@link #getAdmin()}.
	 * Ignored in case of a subsequent {@link #setAdmin(HBaseAdmin)}.
	 * Changed when invoked {@link #start()} or {@link #restart()}.
	 */
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

	private void uncache(String tableName) {
		//tableName = this.mangleTableName(tableName);
		synchronized (this.tablesD) {
			this.tablesD.remove(tableName);
		}
	}

	/**
	 * Resets the connection to HBase.
	 * Also vacuum any cached value about the store (but not Cached elements from {@link Cache}).
	 * This method is thread-safe ; if a restart is already ongoing, the methods block until restart is done.
	 */
	public void restart() {
		synchronized(this.restartMutex) {
			if (this.restarting)
				try {
					this.restartMutex.wait(10000);
					assert !this.restarting;
					return;
				} catch (InterruptedException e) {
					errorLogger.log(Level.WARNING, "Problem while waiting for store restart: " + e.getMessage(), e);
				}
			this.restarting = true;
		}
		try {
			synchronized(this.tablesD) {
				this.tablesD.clear();
			}
			this.config = HBaseConfiguration.create(this.config);
			this.admin = null;
			this.wasStarted = false;
			this.start();
		} finally {
			synchronized (this.restartMutex) {
				this.restartMutex.notifyAll();
				this.restarting = false;
			}
		}
	}
	
	protected void handleProblem(Throwable e, String table, String... expectedFamilies) throws DatabaseNotReachedException {		
		while (e instanceof UndeclaredThrowableException)
			e = ((UndeclaredThrowableException)e).getCause();
		
		if (e instanceof DatabaseNotReachedException)
			throw (DatabaseNotReachedException)e;
		
		if ((e instanceof TableNotFoundException) || e.getMessage().contains(TableNotFoundException.class.getSimpleName())) {
			table = this.mangleTableName(table);
			errorLogger.log(Level.INFO, "Trying to recover from exception for store " + this.hashCode() + " it seems that a table was dropped ; restarting store", e);
			HTableDescriptor td = this.tablesD.get(table);
			if (td != null) {
				this.uncache(table);
				Set<String> expectedFams = new TreeSet<String>(Arrays.asList(expectedFamilies));
				for (HColumnDescriptor cd : td.getColumnFamilies()) {
					expectedFams.add(cd.getNameAsString());
				}
				this.getTableDescriptor(td.getNameAsString(), expectedFams.toArray(new String[expectedFams.size()]));
			} else
				throw new DatabaseNotReachedException(e);
		} else if ((e instanceof NotServingRegionException) || e.getMessage().contains(NotServingRegionException.class.getSimpleName())) {
			table = this.mangleTableName(table);
			
			try {
				if (this.admin.tableExists(table) && this.admin.isTableDisabled(table)) { //First detect the error
					errorLogger.log(Level.INFO, "It seems that table " + table + " was disabled ; enabling", e);
					synchronized (this.sharedLockTable(table)) {
						try {
							if (this.admin.tableExists(table) && this.admin.isTableDisabled(table)) { //Double check once the lock is acquired
								synchronized(this.exclusiveLockTable(table)) {
									try {
										if (this.admin.tableExists(table) && this.admin.isTableDisabled(table))
											this.admin.enableTable(table);
									} finally {
										this.exclusiveUnlockTable(table);
									}
								}
							}
						} finally {
							this.sharedUnlockTable(table);
						}
					}
				} else
					throw new DatabaseNotReachedException(e);
			} catch (IOException f) {
				throw new DatabaseNotReachedException(f);
			}
		} else if ((e instanceof NoSuchColumnFamilyException) || e.getMessage().contains(NoSuchColumnFamilyException.class.getSimpleName())) {
			table = this.mangleTableName(table);
			errorLogger.log(Level.INFO, "Trying to recover from exception for store " + this.hashCode() + " it seems that table " + table + " was dropped a column family ; restarting store", e);
			HTableDescriptor td = this.tablesD.get(table);
			if (td != null) {
				this.uncache(table);
				Set<String> expectedFams = new TreeSet<String>(Arrays.asList(expectedFamilies));
				for (HColumnDescriptor cd : td.getColumnFamilies()) {
					expectedFams.add(cd.getNameAsString());
				}
				this.getTableDescriptor(td.getNameAsString(), expectedFams.toArray(new String[expectedFams.size()]));
			} else
				throw new DatabaseNotReachedException(e);
		} else if ((e instanceof ConnectException) || e.getMessage().contains(ConnectException.class.getSimpleName())) {
			errorLogger.log(Level.INFO, "Trying to recover from exception for store " + this.hashCode() + " it seems that connection was lost ; restarting store", e);
			HConnectionManager.deleteConnection(this.config, true);
			restart();
		} else if (e instanceof IOException) {
			if (e.getMessage().contains("closed") || (e instanceof ScannerTimeoutException) || e.getMessage().contains("timeout") || (e instanceof UnknownScannerException)) {
				errorLogger.log(Level.INFO, "Trying to recover from exception for store " + this.hashCode() + " ; restarting store", e);
				restart();
				return;
			}
			
			throw new DatabaseNotReachedException(e);
		}
	}
	
	protected <R> R tryPerform(Action<R> action, String tableName, String... expectedFamilies) throws DatabaseNotReachedException {
		HTable table = this.getTable(tableName, expectedFamilies);
		try {
			return this.tryPerform(action, table, expectedFamilies);
		} finally {
			this.returnTable(action.getTable());
		}
		
	}
	
	/**
	 * Performs an action. Table should be replaced by action.getTable() as it can change in case of problem handling (like a connection lost).
	 */
	protected <R> R tryPerform(Action<R> action, HTable table, String... expectedFamilies) throws DatabaseNotReachedException {	
		action.setTable(table);
		try {
			return action.perform();
		} catch (Throwable e) {
			errorLogger.log(Level.INFO, "Got an error while performing a " + action.getClass().getName() + " on table " + Bytes.toString(action.getTable().getTableName()) + " for store " + this.hashCode(), e);
			
			String tableName = Bytes.toString(table.getTableName());
			HTablePool tp = this.tablesC;
			this.handleProblem(e, tableName, expectedFamilies);
			if (tp != this.tablesC) { //Store was restarted ; we should get a new table client
				table = this.getTable(tableName, expectedFamilies);
				action.setTable(table);
			}
			try {
				errorLogger.log(Level.INFO, "Retrying to perform again erroneous " + action.getClass().getName() + " on table " + Bytes.toString(action.getTable().getTableName()) + " for store " + this.hashCode(), e);
				return action.perform();
			} catch (Exception f) {
				errorLogger.log(Level.SEVERE, "Cannot recover from error while performing a" + action.getClass().getName() + " on table " + Bytes.toString(action.getTable().getTableName()) + " for store " + this.hashCode(), e);
				throw new DatabaseNotReachedException(f);
			}
		}
	}
	
	protected SharedExclusiveLock getLock(String table) throws DatabaseNotReachedException {
		table = this.mangleTableName(table);
		synchronized (this.locks) {
			SharedExclusiveLock ret = this.locks.get(table);
			ZooKeeper zk;
			try {
				try {
					zk = this.admin.getConnection().getZooKeeperWatcher().getZooKeeper();
				} catch (NullPointerException x) { //Lost zookeeper ?
					this.restart();
					zk = this.admin.getConnection().getZooKeeperWatcher().getZooKeeper();
				}
				if (!zk.getState().isAlive()) {
					errorLogger.log(Level.WARNING, "Zookeeper connection lost ; restarting...");
					this.restart();
					zk = this.admin.getConnection().getZooKeeperWatcher().getZooKeeper();
				}
			} catch (Exception e1) {
				throw new DatabaseNotReachedException(e1);
			}
			
			if (ret == null) {
				try {
					zk = this.admin.getConnection().getZooKeeperWatcher().getZooKeeper();
					String dir = "/n-orm/schemalock/" + table;
					if (zk.exists("/n-orm", false) == null)  {
						zk.create("/n-orm", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
					if (zk.exists("/n-orm/schemalock", false) == null)  {
						zk.create("/n-orm/schemalock", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
					if (zk.exists(dir, false) == null) {
						String node = zk.create(dir, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						logger.info("Created lock node " + node);
					}
					ret = new SharedExclusiveLock(zk, dir);
					this.locks.put(table, ret);
				} catch (Exception e) {
					throw new DatabaseNotReachedException(e);
				}
			}
			
			if (ret.getZookeeper() != zk) {
				ret.setZookeeper(zk);
			}
			
			return ret;
		}
	}
	
	protected Object sharedLockTable(String table) throws DatabaseNotReachedException {
		table = this.mangleTableName(table);
		SharedExclusiveLock lock;
		try {
			lock = this.getLock(table);
		} catch(Exception x) {
			//Giving up
			logger.log(Level.WARNING, "Cannot get shared lock on " + table + " ; continuing anyway", x);
			return new Object();
		}
		synchronized (lock) {
			assert lock.getCurrentExclusiveLock() == null;
			try {
				lock.getSharedLock(lockTimeout);
				return lock;
			} catch (Exception e) {
				throw new DatabaseNotReachedException(e);
			}
		}
	}
	
	protected void sharedUnlockTable(String table) throws DatabaseNotReachedException {
		table = this.mangleTableName(table);
		SharedExclusiveLock lock = this.getLock(table);
		synchronized (lock) {
			if (lock.getCurrentSharedLock() != null) {
				try {
					lock.releaseSharedLock();
					this.sharedExclusiveLockedTables.remove(table);
					assert lock.getCurrentExclusiveLock() == null;
				} catch (Exception e) {
					errorLogger.log(Level.SEVERE, "Error unlocking table " + table, e);
				}
			}
		}
	}
	
	protected SharedExclusiveLock exclusiveLockTable(String table) throws DatabaseNotReachedException {
		table = this.mangleTableName(table);
		SharedExclusiveLock lock = this.getLock(table);
		synchronized (lock) {
			if (lock.getCurrentSharedLock() != null) {
				this.sharedUnlockTable(table);
				this.sharedExclusiveLockedTables.add(table);
			}
			try {
				lock.getExclusiveLock(lockTimeout);
				assert lock.getCurrentThread() == Thread.currentThread();
				return lock;
			} catch (Exception e) {
				throw new DatabaseNotReachedException(e);
			}
		}
	}
	
	protected void exclusiveUnlockTable(String table) {
		table = this.mangleTableName(table);
		SharedExclusiveLock lock = this.getLock(table);
		synchronized (lock) {
			try {
				if (lock.getCurrentExclusiveLock() != null)
					lock.releaseExclusiveLock();
			} catch (Exception e) {
				errorLogger.log(Level.SEVERE, "Error unlocking table " + table + " locked in exclusion", e);
			} finally {
				if (this.sharedExclusiveLockedTables.contains(table)) {
					this.sharedExclusiveLockedTables.remove(table);
					this.sharedLockTable(table);
				}
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
			boolean ret;
			synchronized(this.sharedLockTable(name)) {
				try {
					ret = this.admin.tableExists(name);
				} finally {
					this.sharedUnlockTable(name);
				}
			}
			if (!ret &&this.tablesD.containsKey(name)) {
				this.uncache(name);
			}
			return ret;
		} catch (Exception e) {
			throw new DatabaseNotReachedException(e);
		}
	}
	
	protected HTableDescriptor getTableDescriptor(String name, String... expectedFamilies) {
		name = this.mangleTableName(name);
		HTableDescriptor td;
		boolean created = false;
		synchronized (this.tablesD) {
			if (!this.tablesD.containsKey(name)) {
				try {
					synchronized(this.sharedLockTable(name)) {
						try {
							logger.fine("Unknown table " + name + " for store " + this.hashCode());
							if (!this.admin.tableExists(name)) {
								logger.info("Table " + name + " not found ; creating with column families " + Arrays.toString(expectedFamilies));
								td = new HTableDescriptor(name);
								if (expectedFamilies != null) {
									for (String fam : expectedFamilies) {
										byte [] famB = Bytes.toBytes(fam);
										if (!td.hasFamily(famB)) {
											HColumnDescriptor famD = new HColumnDescriptor(fam);
											if (this.compression != null)
												famD.setCompressionType(this.compression);
											td.addFamily(famD);
										}
									}
								}
								synchronized(this.exclusiveLockTable(name)) {
									try {
										this.admin.createTable(td);
										logger.info("Table " + name + " created with column families " + Arrays.toString(expectedFamilies));
										created = true;
									} catch (TableExistsException x) {
										//Already done by another process...
										td = this.admin.getTableDescriptor(Bytes.toBytes(name));
										logger.fine("Got descriptor for table " + name);
									} finally {
										this.exclusiveUnlockTable(name);
										assert this.getLock(name).getCurrentSharedLock() != null;
									}
								}
							} else {
								td = this.admin.getTableDescriptor(Bytes.toBytes(name));
								logger.fine("Got descriptor for table " + name);
							}
							this.cache(name, td);
						} finally {
							this.sharedUnlockTable(name);
						}
					}
				} catch (Exception x) {
					throw new DatabaseNotReachedException(x);
				}
			} else {
				td = this.tablesD.get(name);
			}
		}

		if (!created && expectedFamilies != null && expectedFamilies.length>0) {
			this.enforceColumnFamiliesExists(td, expectedFamilies);
		}
		
		return td;
	}

	protected HTable getTable(String name, String... expectedFamilies)
			throws DatabaseNotReachedException {
		name = this.mangleTableName(name);
		
		//Checking that this table actually exists with the expected column families
		this.getTableDescriptor(name, expectedFamilies);
		
		try {
			return (HTable)this.tablesC.getTable(name);
		} catch (Exception x) {
			this.handleProblem(x, name, expectedFamilies);
			this.getTableDescriptor(name, expectedFamilies);
			return (HTable)this.tablesC.getTable(name);
		}
	}
	
	protected void returnTable(HTable table) {
		this.tablesC.putTable(table);
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
			synchronized (this.sharedLockTable(table)) {
				try {
					td = this.admin.getTableDescriptor(Bytes.toBytes(table));
				} catch (Exception e) {
					throw new DatabaseNotReachedException(e);
				} finally {
					this.sharedUnlockTable(table);
				}
			}
			this.cache(table, td);
		}
		return td.hasFamily(Bytes.toBytes(family));
	}

	private void enforceColumnFamiliesExists(HTableDescriptor tableD,
			String... columnFamilies) throws DatabaseNotReachedException {
		assert tableD != null;
		List<HColumnDescriptor> toBeAdded = new ArrayList<HColumnDescriptor>(columnFamilies.length);
		List<HColumnDescriptor> toBeCompressed = new ArrayList<HColumnDescriptor>(columnFamilies.length);
		String tableName = tableD.getNameAsString();
		synchronized (tableD) {
			boolean recreated = false; //Whether table descriptor was just retrieved from HBase admin
			for (String cf : columnFamilies) {
				byte[] cfname = Bytes.toBytes(cf);
				HColumnDescriptor family = tableD.hasFamily(cfname) ? tableD.getFamily(cfname) : null;
				boolean familyExists = family != null;
				boolean hasCorrectCompressor = familyExists ? this.compression == null || !this.forceCompression || family.getCompressionType().equals(this.compression) : true;
				if (!recreated && (!familyExists || !hasCorrectCompressor)) {
					logger.fine("Table " + tableName + " is not known to have family " + cf + " propertly configured: checking from HBase");
					synchronized (this.sharedLockTable(tableName)) {
						try {
							tableD = this.admin.getTableDescriptor(tableD.getName());
						} catch (Exception e) {
							errorLogger.log(Level.INFO, " Problem while getting descriptor for " + tableName + "; retrying", e);
							this.handleProblem(e, tableName);
							this.getTableDescriptor(tableName, columnFamilies);
							return;
						} finally {
							this.sharedUnlockTable(tableName);
						}
					}
					family = tableD.hasFamily(cfname) ? tableD.getFamily(cfname) : null;
					familyExists = family != null;
					hasCorrectCompressor = familyExists ? this.compression == null || !this.forceCompression || family.getCompressionType().equals(this.compression) : true;
					this.cache(tableName, tableD);
					recreated = true;
				}
				if (!familyExists) {
					HColumnDescriptor newFamily = new HColumnDescriptor(cfname);
					if (this.compression != null)
						newFamily.setCompressionType(this.compression);
					toBeAdded.add(newFamily);
				} else if (!hasCorrectCompressor) {
					toBeCompressed.add(family);
				}
			}
			if (!toBeAdded.isEmpty() || !toBeCompressed.isEmpty()) {
				try {
					if (!toBeAdded.isEmpty())
						logger.info("Table " + tableD.getNameAsString() + " is missing families " + toBeAdded.toString() + ": altering");
					if (!toBeCompressed.isEmpty())
						logger.info("Table " + tableD.getNameAsString() + " compressed with " + this.compression + " has the wrong compressor for families " + toBeCompressed.toString() + ": altering");
					synchronized (this.exclusiveLockTable(tableName)) {
						try {
							try {
								this.admin.disableTable(tableD.getName());
							} catch (TableNotFoundException x) {
								this.handleProblem(x, tableName);
								this.admin.disableTable(tableD.getName());
							}
							if (! this.admin.isTableDisabled(tableD.getName()))
								throw new IOException("Not able to disable table " + tableName);
							logger.info("Table " + tableD.getNameAsString() + " disabled");
							for (HColumnDescriptor hColumnDescriptor : toBeAdded) {
								try {
									this.admin.addColumn(tableD.getName(),hColumnDescriptor);
								} catch (TableNotFoundException x) {
									this.handleProblem(x, tableName);
									this.admin.addColumn(tableD.getName(),hColumnDescriptor);
								}
							}
							for (HColumnDescriptor hColumnDescriptor : toBeCompressed) {
								hColumnDescriptor.setCompressionType(this.compression);
								this.admin.modifyColumn(tableD.getName(), hColumnDescriptor);
							}
							boolean done = true;
							do {
								Thread.sleep(10);
								tableD = this.admin.getTableDescriptor(tableD.getName());
								for (int i = 0; done && i < toBeAdded.size(); i++) {
									done = done && tableD.hasFamily(toBeAdded.get(i).getName());
								}
								for (int i = 0; done && i < toBeCompressed.size(); ++i) {
									HColumnDescriptor expectedFamily = toBeCompressed.get(i);
									HColumnDescriptor actualFamily = tableD.getFamily(expectedFamily.getName());
									done = done && actualFamily != null && expectedFamily.getCompressionType().equals(actualFamily.getCompressionType());
								}
							} while (!done);
							this.admin.enableTable(tableD.getName());
							if (! this.admin.isTableEnabled(tableD.getName()))
								throw new IOException("Not able to enable table " + tableName);
							logger.info("Table " + tableD.getNameAsString() + " enabled");
							for (int i = 0; done && i < toBeAdded.size(); i++) {
								if (!tableD.hasFamily(toBeAdded.get(i).getName()))
									throw new IOException("Table " + tableName + " is still lacking familiy " + toBeAdded.get(i).getNameAsString());
							}
							for (int i = 0; done && i < toBeCompressed.size(); ++i) {
								HColumnDescriptor expectedFamily = toBeCompressed.get(i);
								HColumnDescriptor actualFamily = tableD.getFamily(expectedFamily.getName());
								if (actualFamily == null || !expectedFamily.getCompressionType().equals(actualFamily.getCompressionType()))
									throw new IOException("Table " + tableName + " is still having wrong compressor for familiy " + expectedFamily.getNameAsString());
							}
							this.cache(tableName, tableD);
							logger.info("Table " + tableD.getNameAsString() + " altered");
						} finally {
							this.exclusiveUnlockTable(tableName);
						}
					}
				} catch (Exception e) {
					errorLogger.log(Level.SEVERE, "Could not create on table " + tableD.getNameAsString() + " families " + toBeAdded.toString(), e);
					throw new DatabaseNotReachedException(e);
				}

			}
		}
	}
	
	////////////////////////////////////////////////////////////////////
	// Actual implementation methods
	////////////////////////////////////////////////////////////////////

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

	protected Filter createFamilyConstraint(Constraint c) {
		Filter f = null;
		if (c.getStartKey() != null)
			f = new QualifierFilter(CompareOp.GREATER_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes(c.getStartKey())));
		if (c.getEndKey() != null)
			f = this.addFilter(f, new QualifierFilter(CompareOp.LESS_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes(c.getEndKey()))));
		return f;
	}

	protected Scan getScan(Constraint c, String... families) throws DatabaseNotReachedException {
		Scan s = new Scan();
		if (this.scanCaching != null)
			s.setCaching(this.getScanCaching());
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
			s.setFilter(this.addFilter(new FirstKeyOnlyFilter(), new KeyOnlyFilter()));
		}
		
		return s;
	}

	@Override
	public Map<String, Map<String, byte[]>> get(String table, String id,
			Set<String> families) throws DatabaseNotReachedException {
		if (!this.hasTable(table))
			return null;
		
		Get g = new Get(Bytes.toBytes(id));
		for (String family : families) {
			g.addFamily(Bytes.toBytes(family));
		}

		Result r = this.tryPerform(new GetAction(g), table, families.toArray(new String[families.size()]));
		if (r.isEmpty())
			return null;
		
		Map<String, Map<String, byte[]>> ret = new TreeMap<String, Map<String, byte[]>>();
		if (!r.isEmpty()) {
			for (KeyValue kv : r.list()) {
				String familyName = Bytes.toString(kv.getFamily());
				Map<String, byte[]> fam = ret.get(familyName);
				if (fam == null) {
					fam = new TreeMap<String, byte[]>();
					ret.put(familyName, fam);
				}
				fam.put(Bytes.toString(kv.getQualifier()),
						kv.getValue());
			}
		}
		return ret;
	}

	@Override
	public void storeChanges(String table, String id,
			Map<String, Map<String, byte[]>> changed,
			Map<String, Set<String>> removed,
			Map<String, Map<String, Number>> increments)
			throws DatabaseNotReachedException {
		Set<String> families = new HashSet<String>();
		if (changed !=null) families.addAll(changed.keySet());
		if (removed != null) families.addAll(removed.keySet());
		if (increments != null) families.addAll(increments.keySet());
		
		families.add(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME);
		
		String[] famAr = families.toArray(new String[families.size()]);
		HTable t = this.getTable(table, famAr);

		try {
			byte[] row = Bytes.toBytes(id);
			
	
			List<org.apache.hadoop.hbase.client.Row> actions = new ArrayList<org.apache.hadoop.hbase.client.Row>(2);
	
			Put rowPut = null;
			if (changed != null && !changed.isEmpty()) {
				rowPut = new Put(row);
				for (Entry<String, Map<String, byte[]>> family : changed.entrySet()) {
					byte[] cf = Bytes.toBytes(family.getKey());
					for (Entry<String, byte[]> col : family.getValue().entrySet()) {
						rowPut.add(cf, Bytes.toBytes(col.getKey()), col.getValue());
					}
				}
				if (rowPut.getFamilyMap().isEmpty())
					rowPut = null;
				else
					actions.add(rowPut);
			}
	
			Delete rowDel = null;
			if (removed != null && !removed.isEmpty()) {
				rowDel = new Delete(row);
				for (Entry<String, Set<String>> family : removed.entrySet()) {
					byte[] cf = Bytes.toBytes(family.getKey());
					for (String key : family.getValue()) {
						rowDel.deleteColumns(cf, Bytes.toBytes(key));
					}
	
				}
				if (rowDel.getFamilyMap().isEmpty())
					rowDel = null;
				else
					actions.add(rowDel);
			}
			
			Increment rowInc = null;
			if (increments != null && !increments.isEmpty()) {
				rowInc = new Increment(row);
				for (Entry<String, Map<String, Number>> incrs : increments.entrySet()) {
					byte[] cf = Bytes.toBytes(incrs.getKey());
					for (Entry<String, Number> inc : incrs.getValue().entrySet()) {
						rowInc.addColumn(cf, Bytes.toBytes(inc.getKey()), inc.getValue().longValue());
					}
				}
				if (rowInc.getFamilyMap().isEmpty())
					rowInc = null;
				//Can't add that to actions :(
			}
	
			//An empty object is to be stored...
			if (rowPut == null && rowInc == null) { //NOT rowDel == null; deleting an element that becomes empty actually deletes the element !
				rowPut = new Put(row);
				rowPut.add(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME), null, new byte[]{});
				actions.add(rowPut);
			}
			
			
			Action<?> act;
			
			if (! actions.isEmpty()) {
				act = new BatchAction(actions);
				this.tryPerform(act, t, famAr);
				t = act.getTable();
			}
			
			if (rowInc != null) {
				act = new IncrementAction(rowInc);
				this.tryPerform(act, t, famAr);
				t = act.getTable();
			}
		} finally {
			if (t != null)
				this.returnTable(t);
		}
	}
	
	

//	@Override
//	public void delete(PersistingElement elt, String table, String id)
//			throws DatabaseNotReachedException {
//		if (!this.hasTable(table))
//			return;
//		
//		boolean hasIncrements = false;
//		Class<? extends PersistingElement> clazz = elt.getClass();
//		PropertyManagement pm = PropertyManagement.getInstance();
//		for (Field field : pm.getProperties(clazz)) {
//			if (field.isAnnotationPresent(Incrementing.class)) {
//				hasIncrements = true;
//				break;
//			}
//		}
//		if (!hasIncrements) {
//			ColumnFamiliyManagement cf = ColumnFamiliyManagement.getInstance();
//			for (Field field : cf.getColumnFamilies(clazz)) {
//				if (field.isAnnotationPresent(Incrementing.class)) {
//					hasIncrements = true;
//					break;
//				}
//			}
//		}
//		
//		this.delete(table, id, hasIncrements);
//	}

	@Override
	public void delete(String table, String id)
			throws DatabaseNotReachedException {
		if (!this.hasTable(table))
			return;
//		this.delete(table, id, true);
		Delete d = new Delete(Bytes.toBytes(id));
		this.tryPerform(new DeleteAction(d), table);
	}

//	public void delete(String table, String id, boolean flush)
//			throws DatabaseNotReachedException {
//
//		HTable t = this.getTable(table);
//		try {
//			byte[] ident = Bytes.toBytes(id);
//			Delete rowDel = new Delete(ident);
//			this.tryPerform(new DeleteAction(rowDel), t);
//			
//			if (flush) {
//				//In case the sent object has incrementing columns, table MUST be flushed (HBase bug HBASE-3725)
//				//See https://issues.apache.org/jira/browse/HBASE-3725
//				try {
//					t.flushCommits();
//					HRegionLocation rloc = t.getRegionLocation(ident);
//					this.getAdmin().getConnection().getHRegionConnection(rloc.getServerAddress()).flushRegion(rloc.getRegionInfo());
//				} catch (Exception e) {
//					logger.log(Level.WARNING, "Could not flush table " + table + " after deleting " + id, e);
//				}
//			}
//		} finally {
//			this.returnTable(t);
//		}
//	}

	@Override
	public boolean exists(String table, String row, String family)
			throws DatabaseNotReachedException {
		if (!this.hasColumnFamily(table, family))
			return false;

		Get g = new Get(Bytes.toBytes(row)).addFamily(Bytes.toBytes(family));
		g.setFilter(this.addFilter(new FirstKeyOnlyFilter(), new KeyOnlyFilter()));
		return this.tryPerform(new ExistsAction(g), table);
	}

	@Override
	public boolean exists(String table, String row)
			throws DatabaseNotReachedException {
		if (!this.hasTable(table))
			return false;

		Get g = new Get(Bytes.toBytes(row));
		g.setFilter(this.addFilter(new FirstKeyOnlyFilter(), new KeyOnlyFilter()));
		return this.tryPerform(new ExistsAction(g), table);
	}

	@Override
	public byte[] get(String table, String row, String family, String key)
			throws DatabaseNotReachedException {
		if (!this.hasTable(table))
			return null;

		Get g = new Get(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family),
				Bytes.toBytes(key));

		Result result = this.tryPerform(new GetAction(g), table, family);
		
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
		if (!this.hasTable(table))
			return null;

		Get g = new Get(Bytes.toBytes(id)).addFamily(Bytes.toBytes(family));

		if (c != null) {
			g.setFilter(createFamilyConstraint(c));
		}

		Result r = this.tryPerform(new GetAction(g), table, family);
		if (r.isEmpty())
			return null;
		
		Map<String, byte[]> ret = new HashMap<String, byte[]>();
		if (!r.isEmpty()) {
			for (KeyValue kv : r.raw()) {
				ret.put(Bytes.toString(kv.getQualifier()), kv.getValue());
			}
		}
		return ret;
	}

	@Override
	public long count(String table, Constraint c) throws DatabaseNotReachedException {
		if (! this.hasTable(table))
			return 0;
		
		return this.tryPerform(new CountAction(this, this.getScan(c)), table);
	}

	@Override
	public com.googlecode.n_orm.storeapi.CloseableKeyIterator get(String table, Constraint c,
			 int limit, Set<String> families) throws DatabaseNotReachedException {
		if (!this.hasTable(table))
			return new EmptyCloseableIterator();
		
		String[] famAr = families == null ? null : families.toArray(new String[families.size()]);
		Scan s = this.getScan(c, famAr);
		s.setFilter(this.addFilter(s.getFilter(), new PageFilter(limit)));
		
		ResultScanner r = this.tryPerform(new ScanAction(s), table, famAr);
		return new CloseableIterator(this, table, c, limit, families, r, families != null);
	}

	public void truncate(String table, Constraint c) throws DatabaseNotReachedException {
		if (!this.hasTable(table))
			return;
		
		logger.info("Truncating table " + table);
		
		TruncateAction action = new TruncateAction(this, this.getScan(c));
		this.tryPerform(action, table);
		
		logger.info("Truncated table " + table);
	}

	@Override
	public <AE extends PersistingElement, E extends AE> void process(
			final String table, Constraint c, Set<String> families, Class<E> elementClass,
			Process<AE> action, final Callback callback)
			throws DatabaseNotReachedException {
		if (! this.hasTable(table)) {
			if (callback != null)
				callback.processCompleted();
			return;
		}
		final Class<?> actionClass = action.getClass();
		try {
			String[] famAr = families == null ? null : families.toArray(new String[families.size()]);
			//Checking that cf are all there so that process will work
			this.getTableDescriptor(table, famAr);
			final Job job = ActionJob.createSubmittableJob(this, table, this.getScan(c, famAr), action, elementClass, famAr);
			logger.log(Level.FINE, "Runing server-side process " + actionClass.getName() + " on table " + table + " with id " + job.hashCode());
			if (callback != null) {
				new Thread() {

					@Override
					public void run() {
						try {
							if (job.waitForCompletion(false)) {
								callback.processCompleted();
							} else {
								throw new RuntimeException("Unknown reason");
							}
						} catch (Throwable x) {
							errorLogger.log(Level.WARNING, "Could not perform server-side process " + actionClass.getName() + " on table " + table, x);
							if (callback != null) {
								callback.processCompletedInError(x);
							}
						}
					}
					
				}.start();
			} else
				job.submit();

			logger.log(Level.FINE, "Server-side process " + actionClass.getName() + " on table " + table + " with id " + job.hashCode() + " done !");
		} catch (Throwable x) {
			errorLogger.log(Level.WARNING, "Could not perform server-side process " + actionClass.getName() + " on table " + table, x);
			if (callback != null) {
				callback.processCompletedInError(x);
			}
		}
	}

}
