package com.googlecode.n_orm.f_console.f_shell;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import groovy.lang.GroovyShell;
import org.apache.commons.beanutils.ConvertUtils;
import org.codehaus.groovy.control.CompilationFailedException;
import com.googlecode.n_orm.f_console.annotations.Trigger;
import com.googlecode.n_orm.f_console.commands.CommandList;

public class ShellProcessor
{
	private Shell shell;
	private String escapeCommand;
	private CommandList processor;
	private Map<String, Method> processorCommands;
	
	public ShellProcessor(Shell shell)
	{
		this.shell = shell;
		this.escapeCommand = "exit";
		this.processor = new CommandList(shell);
		
		processorCommands = new HashMap<String, Method>();
		for (Method m : processor.getClass().getDeclaredMethods())
		{
			if (m.getAnnotation(Trigger.class) != null)
				processorCommands.put(m.getName(), m);
		}
	}
	
	public List<String> getCommands()
	{
		ArrayList<String> result = new ArrayList<String>();
		result.add(this.escapeCommand);
		result.addAll(processorCommands.keySet());
		return result;
	}
	
	public void treatLine(String text)
	{
		if (text.replaceAll("\\s+$", "").equals(escapeCommand))
			shell.doStop();
		else
		{
//			executeGroovy(text);
			executeManualCommand(text);
		}
	}
	
	public void executeGroovyCommand(String query) throws CompilationFailedException
	{
		GroovyShell shell = new GroovyShell();
		Object value = shell.evaluate(query);
		this.shell.print(value.toString());
	}
	
	public void executeManualCommand(String query)
	{
		String[] tokens = query.split(" ");
		int currentTokenIndex = 0;
		
		while (currentTokenIndex < tokens.length)
		{
			if (processorCommands.containsKey(tokens[currentTokenIndex]))
			{
				String command = tokens[currentTokenIndex];
				currentTokenIndex++;
				try
				{
					Method m = processorCommands.get(command);
					Class<?>[] parameterTypes = m.getParameterTypes();
					
					Object[] params = new Object[parameterTypes.length];
					if (parameterTypes.length > 0)
					{
						if (tokens.length - currentTokenIndex != parameterTypes.length)
						{
							shell.print("Command format error: " + m.toString().substring(
									m.toString().substring(0, m.toString().lastIndexOf("(")).lastIndexOf(".") + 1, // Last "." before parameters
									m.toString().length())
									);
							break;
						}
						else
						{
							for (int i = 0; i < parameterTypes.length; i++)
								params[i] = ConvertUtils.convert(tokens[currentTokenIndex + i], parameterTypes[i]);
							
							currentTokenIndex += parameterTypes.length - 1;
						}
					}
					m.invoke(processor, params);
				}
				catch (Exception e)
				{
					shell.println(e.toString());
				}
			}
			else
			{
				shell.print("n-orm: " + tokens[currentTokenIndex] + ": command not found");
				currentTokenIndex++;
			}
		}
	}
}