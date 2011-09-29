package com.googlecode.n_orm.hbase;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.WildcardFileFilter;

public abstract class RecursiveFileAction {
	
	private final Collection<String> toBeIgnored;
	private final Collection<String> toBeExplored;
	
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
		toBeIgnored = new LinkedList<String>();
		toBeExplored = new LinkedList<String>();
	}
	
	public void addIgnoredFile(String toBeIgnored) {
		this.toBeIgnored.add(toBeIgnored);
	}
	
	public void clearIgnoredFiles() {
		this.toBeIgnored.clear();
	}
	
	public void addExploredFile(String toBeExplored) {
		this.toBeExplored.add(toBeExplored);
	}
	
	public void clear() {
		this.toBeIgnored.clear();
		this.toBeExplored.clear();
	}
	
	public void addFiles(String... files) {
		for (String file : files) {
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
		Iterator<String> exi = this.toBeExplored.iterator();
		while (exi.hasNext()) {
			String f = exi.next();
			if (!shoudIgnore(f)) {
				this.exploreFile(new File(f), r);
			}
			exi.remove();
		}
	}
	
	public boolean acceptFile(File file) {
		return true;
	}
	
	public abstract void manageFile(File f, Report r);

	protected void exploreFile(File f, Report r) {
		String file = f.getPath();
		if (shoudIgnore(file))
			return;

		FileFilter filter = null;
		String filterRest = null;
		boolean inRecurFilter = false;
		int starP = file.indexOf('*'), questionP = file.indexOf('?');
		if (starP >= 0 || questionP >= 0) { //filter detected
			String originalFile = file;
			int filterP = starP >= 0 ? (questionP >= 0 ? Math.min(starP, questionP) : starP) : questionP;
			filterP = file.lastIndexOf(File.separatorChar, filterP);
			String filterStr = file.substring(filterP+1);
			file = file.substring(0, filterP);
			
			filterP = filterStr.indexOf(File.separatorChar);
			if (filterP >= 0) {
				filterRest = filterStr.substring(filterP);
				filterStr = filterStr.substring(0, filterP);
			}
			if ("**".equals(filterStr)) {
				inRecurFilter = true;
			} else if (!"*".equals(filterStr))
				filter = new WildcardFileFilter(filterStr);
			f = new File(file);
			if (! f.isDirectory())
				return;
		}
		
		if (!f.exists())
			return;

		if (f.isDirectory()) {
			for (File sub : f.listFiles()) {
				if (filter == null || filter.accept(sub)) {
					if (sub.isDirectory()) {
						exploreFile(filterRest == null ? sub : new File(sub.getPath()+filterRest), r);
						if (inRecurFilter)
							exploreFile(filterRest == null ? sub : new File(sub.getPath()+File.separatorChar+"**"+filterRest), r);
					} else if (filterRest == null) {
						exploreFile(sub, r);
					}
				}
			}
		} else if (this.acceptFile(f)) {
			this.manageFile(f, r);
			if (r != null)
				r.fileHandled(f);
		} else
			Store.logger.warning("Cannot handle file " + f.getAbsolutePath());
	}

	protected boolean shoudIgnore(String file) {
		for (String ign : this.toBeIgnored) {
			if (FilenameUtils.wildcardMatch(file, ign, IOCase.INSENSITIVE))
				return true;
		}
		return false;
	}

}
