package com.googlecode.n_orm.console.commands;

import org.codehaus.groovy.control.CompilationFailedException;

import com.googlecode.n_orm.console.annotations.Trigger;
import com.googlecode.n_orm.console.shell.Shell;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

public class CommandList
{
	private Shell shell;
	
	public CommandList(Shell shell)
	{
		this.shell = shell;
	}
	
	@Trigger
	public void export(String filePath)
	{
		shell.print("export command called successfully, param=" + filePath);
	}
	
	@Trigger
	public void changePrompt(String newPrompt)
	{
		shell.setPrompt(newPrompt.replaceAll("\\s+$", "") + " ");
	}
	
	@Trigger
	public void groovy() throws CompilationFailedException
	{
		Binding binding = new Binding();
		binding.setVariable("val", new Integer(2));
		GroovyShell shell = new GroovyShell(binding);
		Object value = shell.evaluate("println 'Hello World!'; x = 123; return val * 10");
		this.shell.print("val = " + value.toString());
	}
}