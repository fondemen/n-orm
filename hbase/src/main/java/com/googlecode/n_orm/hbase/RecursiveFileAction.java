package com.googlecode.n_orm.hbase;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Set;
import java.util.TreeSet;

public abstract class RecursiveFileAction {
	
	Set<File> toBeIgnored;
	
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
	
	public void ignore(File toBeIgnored) {
		if (this.toBeIgnored == null)
			this.toBeIgnored = new TreeSet<File>();
		this.toBeIgnored.add(toBeIgnored);
	}
	
	public abstract boolean acceptFile(File file);
	
	public abstract void manageFile(File f, Report r);
	


	public void recursiveManageFile(File file, Report r) {
		if (this.toBeIgnored != null && this.toBeIgnored.contains(file))
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
