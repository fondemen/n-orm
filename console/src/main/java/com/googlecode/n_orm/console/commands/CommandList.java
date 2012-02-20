package com.googlecode.n_orm.console.commands;

import com.googlecode.n_orm.console.shell.Shell;
import com.googlecode.n_orm.consoleannotations.Trigger;

public class CommandList
{
	public static class Bean {
		private String val;
		
		public Bean(String val) {
			this.val = val;
		}
		
		public String getVal() {
			return val;
		}
		
		public Bean getClone() {
			return new Bean(val);
		}
		
		public String toString() {
			return this.getClass().getSimpleName() + '(' + this.val + ')';
		}
	}
	
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
	
	@Trigger
	public Bean getBean(String val) {
		return new Bean(val);
	}
}