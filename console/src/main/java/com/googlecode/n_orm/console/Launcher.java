package com.googlecode.n_orm.console;

import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.console.shell.Shell;
import com.googlecode.n_orm.console.util.PackageExplorer;
import com.googlecode.n_orm.consoleannotations.Trigger;
import com.googlecode.n_orm.operations.ImportExport;

import java.io.IOException;

public class Launcher
{
	/**
	 * @see com.googlecode.n_orm.PersistingMixin#identifierToString(String) identifier to String method
	 */
	@Trigger
	public static <T> T getElement(Class<T> clazz, String identifier) {
		if (identifier.endsWith("}")) {
			
			String fullId = identifier
					.replace(":", KeyManagement.KEY_SEPARATOR)
					.replace("}", KeyManagement.KEY_END_SEPARATOR)
					.replace(";", KeyManagement.ARRAY_SEPARATOR);
			return StorageManagement.getElement(clazz, fullId);
			
		} else {
			StringBuilder fullIdentifier = new StringBuilder();
	
			String[] keys = identifier.split("&");
	
			for (int index = 0; index < keys.length; ) {
				fullIdentifier
						.append(keys[index++])
						.append("\u0001");
	
				if (index < keys.length) {
					fullIdentifier.append("\u0017");
				}
			}
	
			fullIdentifier.append(clazz.getName());
	
			return StorageManagement.getElement(clazz, fullIdentifier.toString());
		}
	}

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
		shell.putEntryMapCommand(Launcher.class.getName(), Launcher.class);
		shell.launch();
	}
}