package com.googlecode.n_orm.console.shell;

import java.io.IOException;
import java.io.PrintStream;
import jline.Completor;
import jline.ConsoleReader;
import jline.MultiCompletor;
import jline.SimpleCompletor;

public class Shell
{
	private static final String[] EMPTY_STRING_ARRAY = new String[0];
	
	private ConsoleReader in;
	private PrintStream out;
	private ShellProcessor shellProcessor;
	private boolean mustStop = true;
	private String prompt;
	
	public Shell() throws IOException
	{
		this.out = System.out;
		this.in = new ConsoleReader();
		this.shellProcessor = new ShellProcessor(this);
		this.prompt = "n-orm$ ";
		
		SimpleCompletor simpleCompletor = new SimpleCompletor(this.shellProcessor.getCommands().toArray(EMPTY_STRING_ARRAY));
//		ArgumentCompletor argCompletor = new ArgumentCompletor(
//				new SimpleCompletor[] {
//						new SimpleCompletor(new String[] {"test"}),
//						new SimpleCompletor(new String[] {"test1", "test2"})
//						});
		
		MultiCompletor multiCompletor = new MultiCompletor(new Completor[] {/*argCompletor, */simpleCompletor});
		this.in.addCompletor(multiCompletor);
	}
	
	public void doStart()
	{
		this.mustStop = false;
	}
	
	public void doStop()
	{
		this.mustStop = true;
	}
	
	public void print(String text)
	{
		out.print(text);
	}
	
	public void println(String text)
	{
		print(text + System.getProperty("line.separator"));
	}
	
	public void setPrompt(String prompt)
	{
		this.prompt = prompt;
	}
	
	public void launch()
	{
		boolean isFirstCommand = true;
		
		this.doStart();
		while (!mustStop)
		{
			try
			{
				if (!isFirstCommand)
					this.println("");
				shellProcessor.treatLine(this.in.readLine(this.prompt));
				isFirstCommand = isFirstCommand && false;
			}
			catch (IOException e)
			{}
		}
	}
}