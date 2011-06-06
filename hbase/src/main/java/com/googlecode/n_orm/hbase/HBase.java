package com.googlecode.n_orm.hbase;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;

/**
 * The HBase store found according to its configuration folder.
 * An example store.properties file is:<br><code>
 * class=com.googlecode.n_orm.hbase.HBase<br>
 * static-accessor=getStore<br>
 * 1=/usr/lib/hadoop-0.20/conf/,/usr/lib/hbase/conf/,!/usr/lib/hadoop/example-confs
 * compression=gz &#35;can be 'none', 'gz', or 'lzo' ; by default 'none' 
 * </code><br>
 * Given files are explored recursively ignoring files given with a ! prefix.
 * Difference with {@link Store} is that jar found in the given folders are added to the classpath so that you don't need to include the HBase client jars in your application.
 * However, if your application is ran within a servlet container (Tomcat, JBoss...), you should care excluding servlet and jsp APIs whom HBase depends on... 
 */
public class HBase extends Store {

	private static Map<String, HBase> knownStores = new HashMap<String, HBase>();
	
	private static Class<?>[] parameters = new Class[] { URL.class };
	private static RecursiveFileAction addJarAction = new RecursiveFileAction() {

		@Override
		public void manageFile(File file, Report r) {
			URLClassLoader sysloader = (URLClassLoader) ClassLoader
					.getSystemClassLoader();
			Class<URLClassLoader> sysclass = URLClassLoader.class;

			try {
				Method method = sysclass
						.getDeclaredMethod("addURL", parameters);
				method.setAccessible(true);
				method.invoke(sysloader, new Object[] { file.toURI().toURL() });
				//System.out.println(file.getName() + " added to the classpath");
			} catch (Throwable t) {
				System.err.println("Warning: could not add jar file "
						+ file.getAbsolutePath());
				t.printStackTrace();
			}
		}

		@Override
		public boolean acceptFile(File file) {
			return file.getName().endsWith(".jar");
		}
	};

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
		
			HBase ret = knownStores.get(commaSeparatedConfigurationFolders);
			
			if (ret == null) {
				addJarAction.clear();
				addJarAction.addFiles(commaSeparatedConfigurationFolders);
				addJarAction.explore(null);
				Properties props = new Properties();
				props.setProperty("commaSeparatedConfigurationFolders", commaSeparatedConfigurationFolders);
				ret = new HBase(props);
				knownStores.put(commaSeparatedConfigurationFolders, ret);
			}
			
			return ret;
		}
	}

	protected HBase(Properties properties) {
		super(properties);
	}
}
