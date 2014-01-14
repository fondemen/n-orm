package com.googlecode.n_orm.console.shell;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.junit.Before;
import org.junit.Test;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.console.commands.CommandList;
import com.googlecode.n_orm.console.commands.CommandList.Bean;

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
		tmp2.remove(sut.getShowCommand());
		tmp2.remove(sut.getNewCommand());
		tmp2.remove(sut.getUpCommand());
		assertTrue(tmp3.containsAll(tmp2));
	}
	
	@Test
	public void treatEmptyCommandTest()
	{
		String command = "";
		this.sut.treatLine(command);
		assertNull(sut.getContextElement());
		assertTrue(sut.getMapShellVariables().isEmpty());
	}
	
	@Test
	public void treatZeroTest()
	{
		String command = sut.getZeroCommand();
		this.sut.treatLine(command);
		assertNull(sut.getContextElement());
	}
	
	@Test
	public void treatResetTest()
	{
		String command = sut.getResetCommand();
		this.sut.treatLine(command);
		assertNull(sut.getContextElement());
		assertTrue(sut.getMapShellVariables().isEmpty());
	}
	
	@Test
	public void treatLineWithArgsTest()
	{
		String command = "changePrompt";
		String args = "newPrompt$";
		
		shell.println("method result: null");
		mapCommands.changePrompt(args);
		replay(shell);
		this.sut.treatLine(command + " " + args);
		verify(shell);
		reset(shell);
	}
	
	@Test
	public void treatLineWithArrayArgsTest()
	{
		String command = "changePromptArray";
		String args = "newPrompt$";
		
		shell.println("method result: null");
		mapCommands.changePromptArray(new String[] {args});
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
		shell.println(resultMessageAffectation);
		replay(shell);
		this.sut.treatLine(affectationCommand);
		verify(shell);
		reset(shell);
		
		assertEquals(0, this.sut.getMapShellVariables().get(varName));

		shell.println("0");
		shell.updateProcessorCommands();
		shell.setPrompt(Shell.DEFAULT_PROMPT_START + ":" + varName + Shell.DEFAULT_PROMPT_END);
		replay(shell);
		this.sut.treatLine(varName);
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
	
	@Test
	public void treatShowTest()
	{
		String command = sut.getShowCommand() + " var";
		
		TreeSet<BookStore> set = new TreeSet<BookStore>();
		set.add(new BookStore("test"));
		
		Map<String, Object> mapShellVariables = new HashMap<String, Object>();
		mapShellVariables.put("var", set);
		sut.setMapShellVariables(mapShellVariables);
		
//		shell.println("Variable type: " + BookStore.class);
//		shell.println("name: test");
//		shell.println("address: null");
		
//		replay(shell);
		this.sut.treatLine(command);
//		verify(shell);
//		reset(shell);
	}
	
	@Test
	public void treatNewTest()
	{
		String command = sut.getNewCommand() + " " + BookStore.class.getName();
		
//		shell.println("Variable type: " + BookStore.class);
//		shell.println("name: test");
//		shell.println("address: null");
		
//		replay(shell);
		this.sut.treatLine(command);
//		verify(shell);
//		reset(shell);
	}
	
	@Test
	public void treatNewWithAffectationTest()
	{
		String command = sut.getNewCommand() + " " + BookStore.class.getName() + " > a";
		
		this.sut.treatLine(command);
		assertTrue(sut.getMapShellVariables().containsKey("a"));
	}
	
	@Test
	public void treatNewWithErrorTest()
	{
		String command = sut.getNewCommand() + " " + BookStore.class.getName() + "uhieg ehuig";
		
		this.sut.treatLine(command);
	}
	
	@Test
	public void treatCommandWithParamsWithSpacesTest()
	{
		String command = sut.getNewCommand() + "\"test test test test\"";
		
		this.sut.treatLine(command);
	}
	
	@Test
	public void treatWithModelClassesTest()
	{
		sut.putEntryMapCommand(StorageManagement.class.getName(), new StorageManagement());
		sut.updateProcessorCommands();
		String command = "findElements";
		this.sut.treatLine(command);
		String command2 = "ofClass " + BookStore.class.getName();
		this.sut.treatLine(command2);
	}
	
	@Test
	public void stackTest() {
		String command = "getBean", command3 = "getClone";
		String args = "avalue";
		
		shell.updateProcessorCommands();
		shell.setPrompt(Shell.DEFAULT_PROMPT_START + ':' + command + Shell.DEFAULT_PROMPT_END);
		shell.println("method result: Bean(" + args + ')');
		replay(shell);
		this.sut.treatLine(command + " " + args);
		this.sut.updateProcessorCommands();
		verify(shell);
		reset(shell);
		Bean b = (Bean) this.sut.getContextElement();
		assertNotNull(b);
		assertEquals(args, b.getVal());
		
		shell.println("method result: " + args);
		replay(shell);
		this.sut.treatLine("getVal");
		verify(shell);
		reset(shell);
		assertSame(b, this.sut.getContextElement());

		shell.updateProcessorCommands();
		shell.setPrompt(Shell.DEFAULT_PROMPT_START + ':' + command + '.' + command3 + Shell.DEFAULT_PROMPT_END);
		shell.println("method result: Bean(" + args + ')');
		replay(shell);
		this.sut.treatLine(command3);
		verify(shell);
		reset(shell);
		Bean b2 = (Bean) this.sut.getContextElement();
		assertEquals(args, b2.getVal());
		assertNotSame(b, b2);

		shell.updateProcessorCommands();
		shell.setPrompt(Shell.DEFAULT_PROMPT_START + ':' + command + Shell.DEFAULT_PROMPT_END);
		shell.println("Bean(" + args + ')');
		replay(shell);
		this.sut.treatLine("up");
		verify(shell);
		reset(shell);
		assertSame(b, this.sut.getContextElement());
		
		shell.updateProcessorCommands();
		shell.setPrompt(Shell.DEFAULT_PROMPT_START + Shell.DEFAULT_PROMPT_END);
		replay(shell);
		this.sut.treatLine("up");
		verify(shell);
		reset(shell);
		assertNull(this.sut.getContextElement());
		
		replay(shell);
		this.sut.treatLine("up");
		verify(shell);
		reset(shell);
		assertNull(this.sut.getContextElement());
	}
}