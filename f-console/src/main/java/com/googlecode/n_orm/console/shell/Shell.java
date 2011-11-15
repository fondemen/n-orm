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
	
	protected boolean isStarted()
	{
		return !this.mustStop;
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
	
	protected String getPrompt()
	{
		return this.prompt;
	}
	
	protected void setOutput(PrintStream out)
	{
		this.out = out;
	}
	
	protected PrintStream getOutput()
	{
		return this.out;
	}
	
	protected void setInput(ConsoleReader in)
	{
		this.in = in;
	}
	
	protected ConsoleReader getInput()
	{
		return this.in;
	}
	
	public void launch()
	{
		boolean isFirstCommand = true;
		String line;
		
		this.doStart();
		while (!mustStop)
		{
			try
			{
				if (!isFirstCommand)
					this.println("");
				
				line = this.in.readLine(this.prompt);
				if (line != null)
					shellProcessor.treatLine(line);
				else
					mustStop = true;
				
				isFirstCommand = isFirstCommand && false;
			}
			catch (IOException e)
			{}
		}
	}
}