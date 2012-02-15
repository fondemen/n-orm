package com.googlecode.n_orm.console.shell;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import com.googlecode.n_orm.console.commands.CommandList;

public class ShellProcessorTest
{
	private ShellProcessor sut;
	private Shell shell = createMock(Shell.class);
	private CommandList mapCommands = new CommandList(shell);
	
	@Before
	public void createSut()
	{
		this.sut = new ShellProcessor(shell);
		this.sut.getMapCommands().put(CommandList.class.getName(), mapCommands);
		this.sut.updateProcessorCommands();
	}
	
	@Test
	public void accessorsMapCommandsTest()
	{
		CommandList tmp = createMock(CommandList.class);
		Map<String, Object> mapCommands = new HashMap<String, Object>();
		mapCommands.put(CommandList.class.getName(), tmp);
		sut.setMapCommands(mapCommands);
		assertEquals(mapCommands, sut.getMapCommands());
	}
	
	@Test
	public void accessorsMapShellVariablesTest()
	{
		Map<String, Object> mapShellVariables = new HashMap<String, Object>();
		mapShellVariables.put("var", 0);
		sut.setMapShellVariables(mapShellVariables);
		assertEquals(mapShellVariables, sut.getMapShellVariables());
	}
	
	@Test
	public void getCommandsTest()
	{
		Method[] tmp1 = mapCommands.getClass().getDeclaredMethods();
		List<String> tmp2 = sut.getCommandsAsString();
		List<String> tmp3 = new ArrayList<String>();
		for (int i = 0; i < tmp1.length; i++)
			tmp3.add(tmp1[i].getName());
		tmp2.remove(sut.getEscapeCommand());
		tmp2.remove(sut.getZeroCommand());
		tmp2.remove(sut.getResetCommand());
		assertTrue(tmp3.containsAll(tmp2));
	}
	
	@Test
	public void treatZeroTest()
	{
		String command = sut.getZeroCommand();
		this.sut.treatLine(command);
		assertTrue(sut.isShellProcessorZeroed());
	}
	
	@Test
	public void treatResetTest()
	{
		String command = sut.getResetCommand();
		this.sut.treatLine(command);
		assertTrue(sut.isShellProcessorZeroed());
		assertTrue(sut.getMapShellVariables().isEmpty());
	}
	
	@Test
	public void treatLineWithArgsTest()
	{
		String command = "changePrompt";
		String args = "newPrompt$";
		
		shell.updateProcessorCommands();
		shell.setPrompt(Shell.DEFAULT_PROMPT_START + Shell.DEFAULT_PROMPT_END);
		mapCommands.changePrompt(args);
		replay(shell);
		this.sut.treatLine(command + " " + args);
		verify(shell);
		reset(shell);
	}
	
	@Test
	public void treatLineExitTest()
	{
		this.sut.treatLine(this.sut.getEscapeCommand());
		assertFalse(shell.isStarted());
	}
	
	@Test
	public void treatLineUnknownCommandTest()
	{
		String unknownCommand = "unknowncommand";
		String errorMessage = "n-orm: " + unknownCommand + ": command not found";
		
		shell.println(errorMessage);
		replay(shell);
		this.sut.treatLine(unknownCommand);
		verify(shell);
		reset(shell);
	}
	
	@Test
	public void treatLineWrongParametersTest()
	{
		String command = "changePrompt";
		String args = "";
		String errorMessage = "Command format error: " + command + "(java.lang.String)";
		
		shell.println(errorMessage);
		replay(shell);
		this.sut.treatLine(command + " " + args);
		verify(shell);
		reset(shell);
	}
	
	@Test
	public void treatLineThrowsExceptionTest()
	{
		this.sut.setMapCommands(null);
		
		String command = "changePrompt";
		String args = "newPrompt$";
		String errorMessage = "n-orm: " + "null" + ": command error";
		
		shell.println(errorMessage);
		replay(shell);
		this.sut.treatLine(command + " " + args);
		verify(shell);
		reset(shell);
	}
	
	@Test
	public void treatLineWithReturnResultTest()
	{
		String command = "getZero";
		String resultMessage = "method result: 0";
		
		shell.updateProcessorCommands();
		shell.setPrompt(Shell.DEFAULT_PROMPT_START + ":" + command + Shell.DEFAULT_PROMPT_END);
		shell.println(resultMessage);
		replay(shell);
		this.sut.treatLine(command);
		verify(shell);
		reset(shell);
	}
	
	@Test
	public void variableAffectationTest()
	{
		String varName = "var";
		String command = "getZero";
		String affectationCommand = command + " > " + varName;
		String resultMessageAffectation = "method result: 0";
		
		shell.updateProcessorCommands();
		shell.setPrompt(Shell.DEFAULT_PROMPT_START + ":" + command + Shell.DEFAULT_PROMPT_END);
		shell.updateProcessorCommands();
		shell.println(resultMessageAffectation);
		replay(shell);
		this.sut.treatLine(affectationCommand);
		verify(shell);
	}
	
	@Test
	public void variableDisplayTest()
	{	
		String displayCommand = "var";
		String resultMessageDisplay = "0";
		Map<String, Object> mapShellVariables = new HashMap<String, Object>();
		mapShellVariables.put(displayCommand, resultMessageDisplay);
		sut.setMapShellVariables(mapShellVariables);
		
		shell.updateProcessorCommands();
		shell.setPrompt(Shell.DEFAULT_PROMPT_START + ":" + displayCommand + Shell.DEFAULT_PROMPT_END);
		shell.println(resultMessageDisplay);
		replay(shell);
		this.sut.treatLine(displayCommand);
		verify(shell);
		reset(shell);
	}
	
	@Test
	public void variableDisplayNullTest()
	{	
		String displayCommand = "var";
		String resultMessageDisplay = displayCommand + " is null";
		Map<String, Object> mapShellVariables = new HashMap<String, Object>();
		mapShellVariables.put(displayCommand, null);
		sut.setMapShellVariables(mapShellVariables);
		
		shell.println(resultMessageDisplay);
		replay(shell);
		this.sut.treatLine(displayCommand);
		verify(shell);
		reset(shell);
	}
}