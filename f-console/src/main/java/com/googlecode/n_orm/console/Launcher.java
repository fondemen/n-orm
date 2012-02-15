package com.googlecode.n_orm.console;

import java.io.IOException;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.console.shell.Shell;

public class Launcher
{
	public static void main(String[] args) throws IOException
	{
		Shell shell = new Shell();
		shell.putEntryMapCommand(StorageManagement.class.getName(), new StorageManagement());
		shell.launch();
	}
}