package com.googlecode.n_orm.hbase;

import java.io.File;
import java.io.FilenameFilter;

public abstract class RecursiveFileAction {
	
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
	
	public abstract boolean acceptFile(File file);
	
	public abstract void manageFile(File f, Report r);
	


	public void recursiveManageFile(File file, Report r) {
		if (!file.canWrite())
			System.err
					.println("WARNING: cannot read " + file.getAbsolutePath());

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
