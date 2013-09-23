package com.googlecode.n_orm.hbase;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

import org.codehaus.plexus.util.DirectoryScanner;

import com.googlecode.n_orm.DatabaseNotReachedException;

/**
 * An HBase {@link Store} starter found according to its configuration folder.
 * An example store.properties file is:<br><code>
 * class=com.googlecode.n_orm.hbase.HBase<br>
 * static-accessor=getStore<br>
 * 1=/usr/lib/hadoop,/usr/lib/hbase,!/usr/lib/hadoop/example-confs
 * </code><br>
 * Given files and directories are explored recursively ignoring files given with a ! prefix. You can also define (positive or negative with a ! prefix) filters using wilcards such as * (any character set), ? (any character), and ** (any sub-directory) can be used both in included and excluded patterns (see {@link DirectoryScanner}), but at least one directory to look in must be defined without wildcard.
 * All available properties for {@link Store} are supported.
 * Most properties can be overloaded at class or column-family level by using the annotation {@link HBaseSchema}.
 * Difference with {@link Store} is that jars found in the given folders are added to the classpath so that you don't need to include the HBase client jars in your application.
 * However, if your application is ran within a servlet container (Tomcat, JBoss...), you should care excluding servlet and jsp APIs whom HBase depends on... 
 * @see Store
 */
public class HBase {
	
	/**
	 * A dependency to be found for HBase to work.
	 */
	public static class HBaseDependency {
		/**
		 * Filter for finding jar containing dependency
		 */
		private final String jarTemplate;
		
		/**
		 * A class to be tested for dependency availability in the classpath
		 */
		private final String testClass;

		private HBaseDependency(String jarTemplate, String testClass) {
			super();
			this.jarTemplate = jarTemplate;
			this.testClass = testClass;
		}
		
		/**
		 * Tests whether this dependency looks available.
		 * In case no test class is defined, returns null.
		 */
		public Boolean hasDependency() {
			if (this.testClass == null)
				return null;
			try {
				ClassLoader.getSystemClassLoader().loadClass(this.testClass);
				return true;
			} catch (ClassNotFoundException x) {
				return false;
			}
		}
	}
	
	public static final HBaseDependency[] HBaseDependencies = {
		new HBaseDependency("zookeeper*.jar,lib/zookeeper*.jar", "org.apache.zookeeper.ZooKeeper"),
		new HBaseDependency("commons-configuration*.jar,lib/commons-configuration*.jar,configuration*.jar,lib/configuration*.jar", "org.apache.commons.configuration.Configuration"),
		new HBaseDependency("commons-codec*.jar,lib/commons-codec*.jar,codec*.jar,lib/codec*.jar", "org.apache.commons.codec.binary.Base64"),
		new HBaseDependency("commons-io*.jar,lib/commons-io*.jar,io*.jar,lib/io*.jar", "org.apache.commons.io.FileUtils"),
		new HBaseDependency("commons-http*.jar,lib/commons-http*.jar,http*.jar,lib/http*.jar", "org.apache.hadoop.conf.Configuration"),
		new HBaseDependency("hadoop*.jar,lib/hadoop*.jar", "org.apache.commons.httpclient.HttpMethod"),
		new HBaseDependency("hbase*.jar", "org.apache.hadoop.hbase.HBaseConfiguration"),
		new HBaseDependency("commons-logging*.jar,lib/commons-logging*.jar", "org.apache.commons.logging.LogFactory"),
		new HBaseDependency("commons-lang*.jar,lib/commons-lang*.jar", "org.apache.commons.lang.StringUtils"),
		new HBaseDependency("log4j*.jar,lib/log4j*.jar", "org.apache.log4j.Logger"),
		new HBaseDependency("jackson*.jar,lib/jackson*.jar", "org.codehaus.jackson.map.JsonMappingException"),
		new HBaseDependency("protobuf*.jar,lib/protobuf*.jar", "com.google.protobuf.Message"),
		new HBaseDependency("guava*.jar,lib/guava*.jar", "com.google.common.base.Predicate"),
		new HBaseDependency("avro*.jar,lib/avro*.jar", "org.apache.avro.io.DatumReader"),
		new HBaseDependency("guava*jar.jar,lib/guava*jar.jar", null), //Loading Hadoop's patched guava yet not checking it's there (only required by CDH)
		new HBaseDependency("hadoop-common*.jar,lib/hadoop-common*.jar", null) //Loading hadoop-common yet not checking it's there (only required by CDH)
	};

	public static final Logger logger;
	public static final Logger errorLogger;

	private static Map<String, Store> knownStores = new HashMap<String, Store>();
	
	private static Class<?>[] parameters = new Class[] { URL.class };
	private static RecursiveFileAction addJarAction = new RecursiveFileAction() {

		@Override
		public void fileFound(File file, Report r) {
			URLClassLoader sysloader = (URLClassLoader) ClassLoader
					.getSystemClassLoader();
			Class<URLClassLoader> sysclass = URLClassLoader.class;

			try {
				Method method = sysclass
						.getDeclaredMethod("addURL", parameters);
				method.setAccessible(true);
				method.invoke(sysloader, new Object[] { file.toURI().toURL() });
				logger.fine(file.getAbsolutePath() + " added to classpath");
			} catch (Throwable t) {
				errorLogger.log(Level.SEVERE, "Warning: could not add jar file "	+ file.getAbsolutePath(), t);
			}
		}

		@Override
		public boolean acceptFile(File file) {
			return file.getName().endsWith(".jar");
		}
	};
	
	static {
		logger = Logger.getLogger(HBase.class.getName());
		errorLogger = Logger.getLogger(HBase.class.getName()+"-err");
		initSimpleLogger(logger, System.out);
		initSimpleLogger(errorLogger, System.err);
	}
	
	private static void initSimpleLogger(Logger logger, PrintStream out) {
		StreamHandler handler = new StreamHandler(out, new SimpleFormatter());
		logger.addHandler(handler);
		for (Handler h : logger.getHandlers()) {
			if (h != handler)
				logger.removeHandler(h);
		}
	}

	/**
	 * Get an HBase store according to a set of comma-separated configuration
	 * folders. Those folders are supposed to have configuration files following
	 * the pattern *-site.xml. Typically, one could state
	 * "/usr/lib/hadoop,/usr/lib/hbase" as the configuration folder. Compared to
	 * {@link Store#getStore(String)}, it appends to the class path any jar
	 * found under the configuration folder. As such, you must not supply HBase
	 * jars in the CLASSPATH.
	 */
	public static Store getStore(String commaSeparatedConfigurationFolders)
			throws IOException {
		synchronized(HBase.class) {
			Store ret = knownStores.get(commaSeparatedConfigurationFolders);
			
			if (ret == null) {
				
				try {
					//Exploiting common configuration
					String cscf = commaSeparatedConfigurationFolders+",!**/*-tests.jar,!**/*slf4j*.jar,"+createFilters(); //no slf4j since n-orm depends on EHCahe, which depends on a (newer) version of slf4j
					addJarAction.clear();
					addJarAction.addFiles(cscf);
					try {
						addJarAction.explore(null);
					} catch (IllegalArgumentException x) {
						throw new DatabaseNotReachedException("Invalid configuration folders specification " + commaSeparatedConfigurationFolders + ": " + x.getMessage());
					}
					checkConfiguration();
				} catch (ClassNotFoundException x) {
					errorLogger.warning("Could not load all necessary classes (" + x.getMessage() + ") ; retrying loading all possible libraries...");
					//Explore all possibilities
					String cscf = commaSeparatedConfigurationFolders; //any jars, anywhere
					addJarAction.clear();
					addJarAction.addFiles(cscf);
					addJarAction.explore(null);
					try {
						checkConfiguration();
					} catch (ClassNotFoundException e) {
						throw new DatabaseNotReachedException("Cannot load necessary jars from " + commaSeparatedConfigurationFolders + " (" + e.getMessage() + ')');
					}
				}
				ret = Store.getStore(commaSeparatedConfigurationFolders);
				knownStores.put(commaSeparatedConfigurationFolders, ret);
			}
			
			return ret;
		}
	}

	private static void checkConfiguration() throws ClassNotFoundException {
		for (HBaseDependency dep : HBaseDependencies) {
			if (Boolean.FALSE.equals(dep.hasDependency()))
				throw new ClassNotFoundException("Cannot find dependency " + dep.jarTemplate +": could not locate class " + dep.testClass);
		}
	}

	private static String createFilters() {
		StringBuffer ret = new StringBuffer();
		for (int i = 0; i < HBaseDependencies.length; ++i) {
			if(!Boolean.TRUE.equals(HBaseDependencies[i].hasDependency())) {
				// Not comparing to FALSE as it can be null (load in any case)
				ret.append(',');
				ret.append(HBaseDependencies[i].jarTemplate);
			}
		}
		return ret.toString();
	}
}
