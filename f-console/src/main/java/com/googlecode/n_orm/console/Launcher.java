package com.googlecode.n_orm.console;

import java.io.IOException;

import com.googlecode.n_orm.console.shell.Shell;

public class Launcher
{
	public static void main(String[] args) throws IOException
	{
		new Shell().launch();
	}
}