package com.googlecode.n_orm.hbase;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class HBase {

	private static Class<?>[] parameters = new Class[] { URL.class };
	private static FilenameFilter dirAndJarFilter = new FilenameFilter() {

		@Override
		public boolean accept(File dir, String name) {
			return name.endsWith(".jar") || new File(dir, name).isDirectory();
		}
	};

	/**
	 * Get an HBase store according to a set of comma-separated configuration
	 * folders. Those folders are supposed to have configuration files following
	 * the pattern *-site.xml. Typically, one could state
	 * "/usr/lib/hbase,/usr/lib/hadoop" as the configuration folder. Compared to
	 * {@link Store#getStore(String)}, it appends to the class path any jar
	 * found under the configuration folder. As such, you must not supply HBase
	 * jars in the CLASSPATH.
	 */
	public static Store getStore(String commaSeparatedConfigurationFolders)
			throws IOException {

		for (String configurationFolder : commaSeparatedConfigurationFolders
				.split(",")) {
			addJarsToClasspath(new File(configurationFolder));
		}
		return Store.getStore(commaSeparatedConfigurationFolders);
	}

	public static void addJarsToClasspath(File file) {
		if (!file.canWrite())
			System.err
					.println("WARNING: cannot read " + file.getAbsolutePath());

		if (file.isDirectory()) {
			for (String name : file.list(dirAndJarFilter)) {
				addJarsToClasspath(new File(file, name));
			}
		} else if (file.getName().endsWith(".jar")) {
			URLClassLoader sysloader = (URLClassLoader) ClassLoader
					.getSystemClassLoader();
			Class<URLClassLoader> sysclass = URLClassLoader.class;

			try {
				Method method = sysclass
						.getDeclaredMethod("addURL", parameters);
				method.setAccessible(true);
				method.invoke(sysloader, new Object[] { file.toURI().toURL() });
				System.out.println(file.getName() + " added to the classpath");
			} catch (Throwable t) {
				System.err.println("Warning: could not add jar file "
						+ file.getAbsolutePath());
				t.printStackTrace();
			}
		} else
			System.err.println("Cannot add file " + file.getAbsolutePath()
					+ " to classpath");
	}
}
