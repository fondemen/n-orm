package com.googlecode.n_orm.console.shell;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.beanutils.ConvertUtils;
import com.googlecode.n_orm.consoleannotations.Trigger;

public class ShellProcessor
{
	// Fields
	private Shell shell;
	private Map<String, Object> mapCommands;
	private Map<String, Method> processorCommands;
	private Map<String, Object> mapShellVariables;
	private String escapeCommand = "quit";
	private String resetCommand = "reset";
	private String affectationCommand = ">";
	private boolean isShellProcessorReseted = true;
	private Object context = null;
	
	public ShellProcessor(Shell shell)
	{
		this.shell = shell;
		this.mapCommands = new HashMap<String, Object>();
		this.mapShellVariables = new HashMap<String, Object>();
		
		this.updateProcessorCommands();
	}
	
	public void updateProcessorCommands()
	{
		processorCommands = new HashMap<String, Method>();
		for (Object o : mapCommands.values())
		{
			for (Method m : o.getClass().getDeclaredMethods())
			{
				if (m.getAnnotation(Trigger.class) != null)
					processorCommands.put(m.getName(), m);
			}
		}
	}

	public String getEscapeCommand()
	{
		return this.escapeCommand;
	}
	
	public String getResetCommand()
	{
		return resetCommand;
	}
	
	public Map<String, Object> getMapCommands()
	{
		return mapCommands;
	}

	public void setMapCommands(Map<String, Object> mapCommands)
	{
		this.mapCommands = mapCommands;
	}
	
	public void putEntryMapCommand(String key, Object value)
	{
		this.mapCommands.put(key, value);
		shell.updateProcessorCommands();
	}
	
	public Map<String, Object> getMapShellVariables()
	{
		return mapShellVariables;
	}

	public void setMapShellVariables(Map<String, Object> mapShellVariables)
	{
		this.mapShellVariables = mapShellVariables;
	}

	public List<String> getCommands()
	{
		ArrayList<String> result = new ArrayList<String>();
		result.add(this.escapeCommand);
		result.add(this.resetCommand);
		result.addAll(processorCommands.keySet());
		return result;
	}
	
	public void treatLine(String text)
	{
		if (text.replaceAll("\\s+$", "").equals(escapeCommand))
			shell.doStop();
		else if (text.replaceAll("\\s+$", "").equals(resetCommand))
		{
			this.doReset();
		}
		else
			executeQuery(text.replaceAll("\\s+", " "));
	}

	private void executeQuery(String query)
	{
		// Get all the tokens of the query
		String[] tokens = query.split(" ");
		int currentTokenIndex = 0;
		
		// Execute every commands in the query
		while (currentTokenIndex < tokens.length)
		{
			// Get the command
			String command = tokens[currentTokenIndex];
			currentTokenIndex++;
			
			// Check if this is a command on an object or on the shell (variable affectation, etc)
			if (command.equals(affectationCommand))
			{
				mapShellVariables.put(tokens[currentTokenIndex], this.context);
				currentTokenIndex++;
			}
			else if (mapShellVariables.containsKey(command)) // If this is an action on a variable of the shell
			{
				if (mapShellVariables.get(command) != null)
				{
					Object context = mapShellVariables.get(command);
					shell.println(context.toString());
					this.context = context;
					
					// Change the prompt of the shell and update the completors
					this.shell.updateProcessorCommands();
					this.shell.setPrompt(Shell.DEFAULT_PROMPT_START + ":" + command + Shell.DEFAULT_PROMPT_END);
				}
				else
					shell.println(command + " is null");
			}
			else if (processorCommands.containsKey(command)) // The command must be registered in the processor
			{
				try
				{
					Method m = processorCommands.get(command);
					Class<?>[] parameterTypes = m.getParameterTypes();
					// Find parameters of the command
					Object[] params = new Object[parameterTypes.length];
					if (parameterTypes.length > 0)
					{
						// Check the format of the command and get the parameters
						if (tokens.length - currentTokenIndex < parameterTypes.length)
						{
							shell.println("Command format error: " + m.toString().substring(
									m.toString().substring(0, m.toString().lastIndexOf("(")).lastIndexOf(".") + 1, // Last "." before parameters
									m.toString().length())
									);
							break;
						}
						else
						{
							for (int i = 0; i < parameterTypes.length; i++)
								params[i] = ConvertUtils.convert(tokens[currentTokenIndex + i], parameterTypes[i]);
							
							currentTokenIndex += parameterTypes.length;
						}
					}
					
					// Execute the command
					this.context = m.invoke(mapCommands.get(m.getDeclaringClass().getName()), params);
					
					// Print the result on the shell (if there is a result)
					if (this.context != null)
					{
						this.isShellProcessorReseted = false;
						// Change the prompt of the shell and update the completors
						this.shell.updateProcessorCommands();
						this.shell.setPrompt(Shell.DEFAULT_PROMPT_START + ":" + command + Shell.DEFAULT_PROMPT_END);
						shell.println("method result: " + this.context.toString());
					}
					else // Reset the shell in this case
						this.doReset();
				}
				catch (Exception e)
				{
					shell.println("n-orm: " + e.getMessage() + ": command error");
				}
			}
			else // The command is unknown, move to the next one in case we know it
			{
				shell.println("n-orm: " + command + ": command not found");
				currentTokenIndex++;
			}
		}
	}
	
	private void doReset()
	{
		this.context = null;
		this.isShellProcessorReseted = true;
		this.shell.updateProcessorCommands();
		this.shell.setPrompt(Shell.DEFAULT_PROMPT_START + Shell.DEFAULT_PROMPT_END);
	}
	
	protected boolean isShellProcessorReseted()
	{
		return this.isShellProcessorReseted;
	}
	
	/*
	 findElements
	 	ofClass
	 		display list of params
	 			withKey *param*					<------------
	 				|setTo *param*							|
	 				|between *param1* and *param2*		-----
	 					go (un jour)
	 
	 */
}