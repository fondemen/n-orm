package com.googlecode.n_orm.console.commands;

import com.googlecode.n_orm.console.shell.Shell;
import com.googlecode.n_orm.consoleannotations.Trigger;

public class CommandList
{
	private Shell shell;
	
	public CommandList(Shell shell)
	{
		this.shell = shell;
	}
	
	@Trigger
	public void changePrompt(String newPrompt)
	{
		shell.setPrompt(newPrompt.replaceAll("\\s+$", "") + " ");
	}
	
	@Trigger
	public int getZero()
	{
		return 0;
	}
	
	@Trigger
	public String getConstantString()
	{
		return "hello world";
	}
}