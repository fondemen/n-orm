package com.googlecode.n_orm.hbase;

import java.io.File;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeSet;

import org.codehaus.plexus.util.DirectoryScanner;

public abstract class RecursiveFileAction {

	private final Collection<String> toBeExplored;
	private final Collection<String> toBeIgnoredFilters;
	private final Collection<String> toBeExploredFilters;
	
	public static abstract class Report {
		public abstract void fileHandled(File f);
	}
	
	public RecursiveFileAction() {
		toBeIgnoredFilters = new LinkedList<String>();
		toBeExploredFilters = new LinkedList<String>();
		toBeExplored = new LinkedList<String>();
	}
	
	private void addFile(Collection<String> list, String element) {
		list.add(element);
//		String modifiedElement = element;
//		if (!modifiedElement.endsWith("/") && !modifiedElement.endsWith("\\"))
//			modifiedElement = modifiedElement+File.separatorChar;
//		//if (!modifiedElement.startsWith("/") && !modifiedElement.startsWith("\\") && !modifiedElement.startsWith(".") && !modifiedElement.contains("**"))
//		//	modifiedElement = "**/"+modifiedElement;
//		if (modifiedElement != element)
//			list.add(modifiedElement);
	}
	
	public void addIgnoredFile(String toBeIgnored) {
		this.addFile(this.toBeIgnoredFilters, toBeIgnored);
	}
	
	public void clearIgnoredFiles() {
		this.toBeIgnoredFilters.clear();
	}
	
	public void addExploredFile(String toBeExplored) {
		if (toBeExplored.contains("*") || toBeExplored.contains("?"))
			this.addFile(this.toBeExploredFilters, toBeExplored);
		else if (new File(toBeExplored).isDirectory())
			this.toBeExplored.add(toBeExplored);
		else
			Store.logger.warning(toBeExplored + " is neither a filter (no *, **, or ? found) nor a valid directory ; ignoring");
	}
	
	public void clear() {
		this.toBeIgnoredFilters.clear();
		this.toBeExploredFilters.clear();
		this.toBeExplored.clear();
	}
	
	public void addFiles(String... files) {
		for (String file : files) {
			file = file.trim();
			if (file.startsWith("!"))
				this.addIgnoredFile(file.substring(1));
			else
				this.addExploredFile(file);
		}
	}
		
	public void addFiles(String comaSeparatedFiles) {
		this.addFiles(comaSeparatedFiles.split(","));
	}
	
	public void explore(Report r) {
		if (this.toBeExplored.isEmpty())
			throw new IllegalArgumentException("No directory found ; please provide at least one directory with a non filter expression (with no *, **, or ?).");
		DirectoryScanner scanner = new DirectoryScanner();
		if (!this.toBeExploredFilters.isEmpty())
			scanner.setIncludes(this.toBeExploredFilters.toArray(new String[0]));
		if (!this.toBeIgnoredFilters.isEmpty())
			scanner.setExcludes(this.toBeIgnoredFilters.toArray(new String[0]));
		scanner.setCaseSensitive(false);
		
		Set<String> found = new TreeSet<String>();
		for (String tbe : this.toBeExplored) {
			scanner.setBasedir(tbe);
			scanner.scan();
			for (String ff : scanner.getIncludedFiles()) {
				found.add(tbe+File.separatorChar+ff);
			}
		}
		
 		for (String file : found) {
			File f = new File(file);
			if (this.acceptFile(f)) {
				this.manageFile(f, r);
				if (r != null)
					r.fileHandled(f);
			}
		}
	}
	
	public boolean acceptFile(File file) {
		return true;
	}
	
	public abstract void manageFile(File f, Report r);

}
