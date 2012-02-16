package com.googlecode.n_orm.console.shell;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.PropertyUtils;
import com.googlecode.n_orm.consoleannotations.Continuator;
import com.googlecode.n_orm.consoleannotations.Trigger;

public class ShellProcessor
{
	// Fields
	private Shell shell;
	private Map<String, Object> mapCommands;
	private Map<String, Method> processorCommands;
	private Map<String, Object> mapShellVariables;
	private String escapeCommand = "quit";
	private String zeroCommand = "zero";
	private String resetCommand = "reset";
	private String showCommand = "show";
	private String newCommand = "new";
	private String affectationCommand = ">";
	private boolean isShellProcessorZeroed = true;
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
		// List all variables
		Iterator<Entry<String, Object>> it = mapShellVariables.entrySet().iterator();
		while (it.hasNext())
			processorCommands.put(it.next().getKey(), null);
		
		if (this.isShellProcessorZeroed)
		{
			// List all Trigger annotated methods
			for (Object o : mapCommands.values())
			{
				for (Method m : o.getClass().getDeclaredMethods())
				{
					if (m.getAnnotation(Trigger.class) != null)
						processorCommands.put(m.getName(), m);
				}
			}
		}
		else
		{
			// List all Continuator annotated methods
			for (Method m : this.context.getClass().getDeclaredMethods())
			{
				if (m.getAnnotation(Continuator.class) != null)
					processorCommands.put(m.getName(), m);
			}
			
			// List all properties of the context and add them as Continuators
			for (Field p : this.context.getClass().getDeclaredFields())
			{
				try
				{
					Method m = PropertyUtils.getReadMethod(PropertyUtils.getPropertyDescriptor(this.context, p.getName()));
					if (m != null)
						processorCommands.put(p.getName(), m);
				}
				catch (Exception e) { }
			}
		}
	}

	public String getEscapeCommand()
	{
		return this.escapeCommand;
	}
	
	public String getZeroCommand()
	{
		return zeroCommand;
	}
	
	public String getResetCommand()
	{
		return resetCommand;
	}
	
	public String getShowCommand()
	{
		return showCommand;
	}
	
	public String getNewCommand()
	{
		return newCommand;
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

	public List<String> getCommandsAsString()
	{
		ArrayList<String> result = new ArrayList<String>();
		result.add(this.escapeCommand);
		result.add(this.zeroCommand);
		result.add(this.resetCommand);
		result.add(this.showCommand);
		result.add(this.newCommand);
		result.addAll(processorCommands.keySet());
		return result;
	}
	
	public List<Method> getCommandsAsMethod()
	{
		ArrayList<Method> result = new ArrayList<Method>();
		result.addAll(processorCommands.values());
		return result;
	}
	
	public void treatLine(String text)
	{
		String textToTreat = text.replaceAll("\\s+", " ");
		String textWithoutSpaceAtTheEnd = textToTreat.replaceAll("\\s+$", "");
		if (textWithoutSpaceAtTheEnd.equals(escapeCommand))
			shell.doStop();
		else if (textWithoutSpaceAtTheEnd.equals(zeroCommand))
			this.doZero();
		else if (textWithoutSpaceAtTheEnd.equals(resetCommand))
			this.doReset();
		else if (textWithoutSpaceAtTheEnd.equals(showCommand))
			this.doShow(textToTreat);
		else if (textWithoutSpaceAtTheEnd.equals(newCommand))
			this.doNew(textToTreat);
		else
			executeQuery(textToTreat);
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
				// Update the completors
				this.shell.updateProcessorCommands();
			}
			else if (mapShellVariables.containsKey(command)) // If this is an action on a variable of the shell
			{
				if (mapShellVariables.get(command) != null)
				{
					Object context = mapShellVariables.get(command);
					shell.println(context.toString());
					this.context = context;
					this.isShellProcessorZeroed = false;
					
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
					if (this.context != null)
						this.context = m.invoke(this.context, params);
					else
						this.context = m.invoke(mapCommands.get(m.getDeclaringClass().getName()), params);
					
					// Print the result on the shell (if there is a result)
					if (this.context != null)
					{
						this.isShellProcessorZeroed = false;
						// Change the prompt of the shell and update the completors
						this.shell.updateProcessorCommands();
						this.shell.setPrompt(Shell.DEFAULT_PROMPT_START + ":" + command + Shell.DEFAULT_PROMPT_END);
						shell.println("method result: " + this.context.toString());
					}
					else // Zero the shell in this case
						this.doZero();
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
	
	private void doZero()
	{
		this.context = null;
		this.isShellProcessorZeroed = true;
		this.shell.updateProcessorCommands();
		this.shell.setPrompt(Shell.DEFAULT_PROMPT_START + Shell.DEFAULT_PROMPT_END);
	}
	
	private void doReset()
	{
		this.mapShellVariables.clear();
		doZero();
	}
	
	private void doShow(String textToTreat)
	{
		
	}
	
	private void doNew(String textToTreat)
	{
		
	}
	
	protected boolean isShellProcessorZeroed()
	{
		return this.isShellProcessorZeroed;
	}
	
	/*
	 findElements
	 	ofClass
	 		display list of params
	 			withKey *param*					<------------
	 				|setTo *param*							|
	 				|between *param1* and *param2*		-----
	 					go (un jour)
	 
	 charger juste les classes annotées Persisting (laisser le chemin complet)
	 
	 new Personne > a
	 a name
	 
	 show result => affiche propriétés dans un tableau (beanutils)
	 
	 */
}