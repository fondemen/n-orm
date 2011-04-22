package com.googlecode.n_orm.hbase;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class HBase {

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
	 * "/usr/lib/hbase,/usr/lib/hadoop" as the configuration folder. Compared to
	 * {@link Store#getStore(String)}, it appends to the class path any jar
	 * found under the configuration folder. As such, you must not supply HBase
	 * jars in the CLASSPATH.
	 */
	public static Store getStore(String commaSeparatedConfigurationFolders)
			throws IOException {

		for (String configurationFolder : commaSeparatedConfigurationFolders
				.split(",")) {
			addJarAction.recursiveManageFile(new File(configurationFolder), null);
		}
		return Store.getStore(commaSeparatedConfigurationFolders);
	}
}
