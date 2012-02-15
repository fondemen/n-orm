package com.googlecode.n_orm.console.shell;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.MultiCompletor;
import jline.SimpleCompletor;

import com.googlecode.n_orm.console.util.PackageExplorer;

public class Shell
{
	private static final String[] PACKAGES_TO_EXPLORE = new String[]
		{
			"com.googlecode.n_orm"
		};
	private static final String[] EMPTY_STRING_ARRAY = new String[0];
	public static final String DEFAULT_PROMPT_START = "n-orm";
	public static final String DEFAULT_PROMPT_END = "$ ";
	
	private ConsoleReader in;
	private PrintStream out;
	private ShellProcessor shellProcessor;
	private boolean mustStop = true;
	private String prompt;
	private Map<String, String> mapClassNames;
	
	public Shell() throws IOException
	{
		this.out = System.out;
		this.in = new ConsoleReader();
		this.shellProcessor = new ShellProcessor(this);
		this.prompt = DEFAULT_PROMPT_START + "$ ";
		
		this.updateProcessorCommands();
		
		this.mapClassNames = this.findAllClassNames();
	}
	
	@SuppressWarnings("rawtypes")
	private Map<String, String> findAllClassNames()
	{
		HashMap<String, String> result = new HashMap<String, String>();
		try
		{	
			List<Class> tmp = new ArrayList<Class>();
			for (String s : PACKAGES_TO_EXPLORE)
				tmp.addAll(PackageExplorer.getClasses(s));
			
			for (Class c : tmp)
				result.put(c.getSimpleName(), c.getName());
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public void updateProcessorCommands()
	{
		// Update the shell processor
		this.shellProcessor.updateProcessorCommands();
		
		// Get all the commands of the shell processor
		ArrayList<String> methodAsString = new ArrayList<String>();
		ArgumentCompletor argCompletor = null;
		
		// Try to see if we don't need a Class as a parameter
		List<Method> processorMethods = this.shellProcessor.getCommandsAsMethod();
		for (Method m : processorMethods)
		{
			if (m != null)
			{
				if (m.getParameterTypes().length > 0)
				{
					for (Class<?> c : m.getParameterTypes())
					{
						if (c.getClass().equals(Class.class))
						{ // In this case we make an argument completor with the name of all the classes of the project
							argCompletor = new ArgumentCompletor(
									new SimpleCompletor[] {
											new SimpleCompletor(new String[] {m.getName()}),
											new SimpleCompletor(this.mapClassNames.keySet().toArray(EMPTY_STRING_ARRAY))
											});
						}
						else
							methodAsString.add(m.getName());
					}
				}
				else
				{
					methodAsString.add(m.getName());
				}
			}
		}
		
		// Add the shell processor variables if needed
		if (shellProcessor.isShellProcessorZeroed())
			methodAsString.addAll(shellProcessor.getMapShellVariables().keySet());
		
		// Add the shell processor commands
		methodAsString.add(shellProcessor.getEscapeCommand());
		methodAsString.add(shellProcessor.getZeroCommand());
		methodAsString.add(shellProcessor.getResetCommand());
		
		// Create the completors
		SimpleCompletor simpleCompletor = new SimpleCompletor(methodAsString.toArray(EMPTY_STRING_ARRAY));
		MultiCompletor multiCompletor = null;
		if (argCompletor == null)
			multiCompletor = new MultiCompletor(new Completor[] {simpleCompletor});
		else
			multiCompletor = new MultiCompletor(new Completor[] {argCompletor, simpleCompletor});
		
		// Update jline completor
		Completor[] listCompletor;
		listCompletor = (Completor[]) this.in.getCompletors().toArray(new Completor[0]);
		for (int i = 0; i < listCompletor.length; i++)
			if (this.in.removeCompletor(listCompletor[i]))
				i--;
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
	
	public String getCurrentPrompt()
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
	
	protected ShellProcessor getShellProcessor()
	{
		return shellProcessor;
	}

	protected void setShellProcessor(ShellProcessor shellProcessor)
	{
		this.shellProcessor = shellProcessor;
	}

	public Map<String, Object> getMapCommands()
	{
		if (this.shellProcessor != null)
			return this.shellProcessor.getMapCommands();
		else
			return null;
	}

	public void setMapCommands(Map<String, Object> mapCommands)
	{
		if (this.shellProcessor != null)
			this.shellProcessor.setMapCommands(mapCommands);	
	}
	
	public void putEntryMapCommand(String key, Object value)
	{
		if (this.shellProcessor != null)
			this.shellProcessor.putEntryMapCommand(key, value);
	}
	
	public Map<String, String> getMapClassNames()
	{
		return mapClassNames;
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
			catch (Exception e)
			{
				println("n-orm: " + e.getMessage() + ": command error");
				mustStop = true;
			}
		}
	}
}