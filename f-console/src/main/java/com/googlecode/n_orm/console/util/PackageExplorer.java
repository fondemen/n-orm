package com.googlecode.n_orm.console.util;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class PackageExplorer {
	
	/**
	 * This method allows us to list all the classes of a package
	 * 
	 * @param pckgname The name of the package to explore
	 * @return The list of the classes contained
	 */
	@SuppressWarnings("rawtypes")
	public static List<Class> getClasses(String pckgname) throws ClassNotFoundException, IOException {
		// Création de la liste qui sera retournée
		ArrayList<Class> classes = new ArrayList<Class>();
	 
		// On récupère toutes les entrées du CLASSPATH
		String [] entries = System.getProperty("java.class.path")
						.split(System.getProperty("path.separator"));
	 
		// Pour toutes ces entrées, on verifie si elles contiennent
		// un répertoire ou un jar
		for (int i = 0; i < entries.length; i++) {
			if(entries[i].endsWith(".jar")){
				classes.addAll(traitementJar(entries[i], pckgname));
			}else{
				classes.addAll(traitementRepertoire(entries[i], pckgname));
			}
	 
		}
	 
		return classes;
	}
	 
	/**
	 * Cette méthode retourne la liste des classes présentes
	 * dans un répertoire du classpath et dans un package donné
	 * 
	 * @param repertoire Le répertoire où chercher les classes
	 * @param pckgname Le nom du package
	 * @return La liste des classes
	 */
	@SuppressWarnings("rawtypes")
	private static Collection<Class> traitementRepertoire(String repertoire, String pckgname) throws ClassNotFoundException {
		ArrayList<Class> classes = new ArrayList<Class>();
	 
		// On génère le chemin absolu du package
		StringBuffer sb = new StringBuffer(repertoire);
		String[] repsPkg = pckgname.split("\\.");
		for (int i = 0; i < repsPkg.length; i++) {
			sb.append(System.getProperty("file.separator") + repsPkg[i]);
		}
		File rep = new File(sb.toString());
	 
		// Si le chemin existe, et que c'est un dossier, alors, on le liste
		if(rep.exists() && rep.isDirectory()){
			// On filtre les entrées du répertoire
			FilenameFilter filter = new DotClassFilter();
			File[] liste = rep.listFiles(filter );
	 
			// Pour chaque classe présente dans le package, on l'ajoute à la liste
			for (int i = 0; i < liste.length; i++) {
				classes.add(Class.forName(pckgname + "." + liste[i].getName().split("\\.")[0]));
			}
		}
	 
		return classes;
	}
	 
	/**
	 * Cette méthode retourne la liste des classes présentes dans un jar du classpath et dans un package donné
	 *
	 * @param repertoire Le jar où chercher les classes
	 * @param pckgname Le nom du package
	 * @return La liste des classes
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	@SuppressWarnings("rawtypes")
	private static Collection<Class> traitementJar(String jar, String pckgname) throws IOException {
		ArrayList<Class> classes = new ArrayList<Class>();
	 
		JarFile jfile = new JarFile(jar);
		String pkgpath = pckgname.replace(".", "/");
		
		// Pour chaque entrée du Jar
		for (Enumeration<JarEntry> entries = jfile.entries(); entries.hasMoreElements();) {
			try {
				JarEntry element = entries.nextElement();
				// Si le nom de l'entrée commence par le chemin du package et finit par .class
				if(element.getName().startsWith(pkgpath)
					&& element.getName().endsWith(".class") && !element.getName().contains("$")){
					String nomFichier = element.getName().substring(pckgname.length() + 1);
		 
					classes.add(Class.forName(pckgname + "." + nomFichier.split("\\.")[0]));
		 
				}
			}
			catch (Exception e)
			{ }
		}
	 
		return classes;
	}
	 
	/**
	 * Cette classe permet de filtrer les fichiers d'un répertoire. Il n'accepte que les fichiers .class.
	 */
	private static class DotClassFilter implements FilenameFilter{
	 
		public boolean accept(File arg0, String arg1) {
			return arg1.endsWith(".class");
		}
	 
	 
	}
}