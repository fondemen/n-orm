package com.googlecode.n_orm.console.util;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Level;

import org.apache.commons.beanutils.ConvertUtils;

public class PackageExplorer
{	
	private static Collection<String> cpLocations;
	
	static {
		// Get all the entries from the CLASSPATH
		String [] entries = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
		cpLocations = new ArrayList<String>(entries.length);
		for (String cpEntry : entries) {
			cpLocations.add(cpEntry);
		}
	}
	
	public static void addSearchEntry(String classPathEntry) {
		File f = new File(classPathEntry);
		if (!f.exists()) {
			System.err.println(classPathEntry + " is not a source location");
			return;
		} else if (!f.isDirectory() && (f.isFile() && !classPathEntry.endsWith(".jar"))) {
			System.err.println(classPathEntry + " is neither a directory nor a JAR file");
			return;
		}
			
		cpLocations.add(classPathEntry);
		Class<URLClassLoader> sysclass = URLClassLoader.class;
		URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
		try {
			Method method = sysclass
					.getDeclaredMethod("addURL", new Class[] { URL.class });
			method.setAccessible(true);
			method.invoke(sysloader, new Object[] { f.toURI().toURL() });
		} catch (Throwable t) {
			System.err.println("Warning: could not add source location " + classPathEntry);
		}
	}
	
	public static Set<String> getLocations() {
		return new TreeSet<String>(cpLocations);
	}
	
	/**
	 * This method allows us to list all the classes of a package (and sub packages)
	 * 
	 * @param pckgName The name of the package to explore
	 * @return The list of the classes contained
	 */
	@SuppressWarnings("rawtypes")
	public static List<Class> getClasses(String pckgName)
	{
		// Create the result list
		ArrayList<Class> classes = new ArrayList<Class>();
		
		// For each entry, check if this is a directory or a jar
		for (String cpLocation : cpLocations)
		{
			if (cpLocation.endsWith(".jar"))
				classes.addAll(treatJar(cpLocation, pckgName));
			else
				classes.addAll(treatDirectory(cpLocation, pckgName));
		}
	 
		return classes;
	}
	 
	/**
	 * This method returns the list of the classes present
	 * in a directory of the classpath in a specified package
	 * 
	 * @param directory The directory where to look for classes
	 * @param pckgName The name of the package
	 * @return The list of the classes
	 */
	@SuppressWarnings("rawtypes")
	private static Collection<Class> treatDirectory(String directory, String pckgName)
	{
		ArrayList<Class> classes = new ArrayList<Class>();
		String fileSeparator = System.getProperty("file.separator");
		
		// Generation of the absolute path of the package
		StringBuffer sb = new StringBuffer(directory);
		String[] repsPkg = pckgName.split("\\.");
		for (int i = 0; i < repsPkg.length; i++)
		{
			sb.append(fileSeparator + repsPkg[i]);
		}
		File dir = new File(sb.toString());
		
		// If the path exists and if it is a directory, we list it
		if (dir.exists() && dir.isDirectory())
		{
			addClassesToListForDirectory(classes, dir, pckgName);
		}
	 
		return classes;
	}
	
	@SuppressWarnings("rawtypes")
	private static void addClassesToListForDirectory(List<Class> classes, File dir, String pckgName)
	{
		if (pckgName.length() > 0 && !pckgName.endsWith("."))
			pckgName = pckgName + ".";
		
		for (File f : dir.listFiles())
		{
			if (f.isDirectory())
				addClassesToListForDirectory(classes, f, pckgName + f.getName());
		}
		
		// We filter entries
		FilenameFilter filter = new DotClassFilter();
		File[] list = dir.listFiles(filter);
		
		// We add each class of the package to the list
		for (int i = 0; i < list.length; i++)
		{
			try
			{
				classes.add((Class) ConvertUtils.convert(pckgName + list[i].getName().split("\\.")[0], Class.class));
			}
			catch (Exception e) { }
		}
	}
	 
	/**
	 * This method returns the list of the classes present
	 * in a jar file of the classpath in a specified package
	 *
	 * @param jar The jar where to look for classes
	 * @param pckgName The name of the package
	 * @return The list of the classes
	 */
	@SuppressWarnings("rawtypes")
	private static Collection<Class> treatJar(String jar, String pckgName)
	{
		ArrayList<Class> classes = new ArrayList<Class>();
		
		try
		{
			JarFile jFile = new JarFile(jar);
			String pkgPath = pckgName.replace(".", "/");
			
			// For each entry of the Jar
			for (Enumeration<JarEntry> entries = jFile.entries(); entries.hasMoreElements(); )
			{
				try
				{
					JarEntry element = entries.nextElement();
					// If the name of the entry start with the package path and ends with .class
					if (element.getName().startsWith(pkgPath) && element.getName().endsWith(".class"))
					{
						String nomFichier = element.getName();
						nomFichier = nomFichier.substring(0, nomFichier.length()-6);
						nomFichier = nomFichier.replace("/", ".");
						classes.add((Class) ConvertUtils.convert(nomFichier, Class.class));
					}
				}
				catch (Exception e) { }
			}
		} catch (IOException e) { }
	 
		return classes;
	}
	 
	/**
	 * This class allows to filter the files of a directory. It accepts only .class files.
	 */
	private static class DotClassFilter implements FilenameFilter
	{
		public boolean accept(File arg0, String arg1)
		{
			return arg1.endsWith(".class");
		}
	}
}