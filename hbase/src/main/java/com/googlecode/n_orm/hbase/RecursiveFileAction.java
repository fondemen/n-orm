package com.googlecode.n_orm.hbase;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

public abstract class RecursiveFileAction {
	
	private final Collection<File> toBeIgnored;
	private final Collection<File> toBeExplored;
	
	public static abstract class Report {
		public abstract void fileHandled(File f);
	}

	private FilenameFilter filter = new FilenameFilter() {
	
		@Override
		public boolean accept(File dir, String name) {
			File f = new File(dir, name);
			return acceptFile(f) || f.isDirectory();
		}
	};
	
	public RecursiveFileAction() {
		toBeIgnored = new LinkedList<File>();
		toBeExplored = new LinkedList<File>();
	}
	
	public void addIgnoredFile(File toBeIgnored) {
		this.toBeIgnored.add(toBeIgnored);
	}
	
	public void clearIgnoredFiles() {
		this.toBeIgnored.clear();
	}
	
	public void addExploredFile(File toBeExplored) {
		this.toBeExplored.add(toBeExplored);
	}
	
	public void clear() {
		this.toBeIgnored.clear();
		this.toBeExplored.clear();
	}
	
	public void addFiles(String... files) {
		for (String file : files) {
			if (file.startsWith("!"))
				this.addIgnoredFile(new File(file.substring(1)));
			else
				this.addExploredFile(new File(file));
		}
	}
		
	public void addFiles(String comaSeparatedFiles) {
		this.addFiles(comaSeparatedFiles.split(","));
	}
	
	public void explore(Report r) {
		Iterator<File> exi = this.toBeExplored.iterator();
		while (exi.hasNext()) {
			File f = exi.next();
			if (!this.toBeIgnored.contains(f)) {
				this.recursiveManageFile(f, r);
			}
			exi.remove();
		}
	}
	
	public abstract boolean acceptFile(File file);
	
	public abstract void manageFile(File f, Report r);
	
	

	protected void recursiveManageFile(File file, Report r) {
		if (this.toBeIgnored.contains(file))
			return;
		
		if (!file.canRead()) {
			System.err.println("WARNING: cannot read " + file.getAbsolutePath());
			return;
		}

		if (file.isDirectory()) {
			for (String name : file.list(this.filter)) {
				recursiveManageFile(new File(file, name), r);
			}
		} else if (this.acceptFile(file)) {
			this.manageFile(file, r);
			if (r != null)
				r.fileHandled(file);
		} else
			System.err.println("Cannot add file " + file.getAbsolutePath()
					+ " to classpath");
	}

}
