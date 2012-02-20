package com.googlecode.n_orm.console;

import java.io.IOException;
import java.util.Date;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.console.shell.Shell;

public class Launcher
{
	public static void main(String[] args) throws IOException
	{	
		Shell shell = new Shell();
		shell.putEntryMapCommand(StorageManagement.class.getName(), StorageManagement.aspectOf());
		shell.launch();
	}
}