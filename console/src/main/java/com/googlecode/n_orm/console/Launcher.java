package com.googlecode.n_orm.console;

import java.io.IOException;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.console.shell.Shell;
import com.googlecode.n_orm.console.util.PackageExplorer;
import com.googlecode.n_orm.operations.ImportExport;

public class Launcher
{
	public static void main(String[] args) throws IOException
	{	
		if (args.length == 0)
			System.out.println("Note: you can add parameters describing locations for jar files and class source directories as parameters so that persistring classes can be found");

		for (String string : args) {
			PackageExplorer.addSearchEntry(string);
		}
		
		Shell shell = new Shell();
		shell.putEntryMapCommand(StorageManagement.class.getName(), StorageManagement.aspectOf());
		shell.putEntryMapCommand(ImportExport.class.getName(), ImportExport.class);
		shell.launch();
	}
}