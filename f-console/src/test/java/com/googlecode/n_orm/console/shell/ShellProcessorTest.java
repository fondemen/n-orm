package com.googlecode.n_orm.console.shell;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import com.googlecode.n_orm.console.commands.CommandList;
import com.googlecode.n_orm.console.shell.Shell;
import com.googlecode.n_orm.console.shell.ShellProcessor;

public class ShellProcessorTest
{
	private ShellProcessor sut;
	private Shell shell = createMock(Shell.class);
	private CommandList commandList = createMockBuilder(CommandList.class).withConstructor(Shell.class).withArgs(shell).createMock();
	
	@Before
	public void createSut()
	{
		this.sut = new ShellProcessor(shell);
	}
	
	@Test
	public void accessorsCommandListTest()
	{
		CommandList tmp = createMock(CommandList.class);
		sut.setCommandList(tmp);
		assertEquals(tmp, sut.getCommandList());
	}
	
	@Test
	public void getCommandsTest()
	{
		Method[] tmp1 = commandList.getClass().getDeclaredMethods();
		List<String> tmp2 = sut.getCommands();
		List<String> tmp3 = new ArrayList<String>();
		for (int i = 0; i < tmp1.length; i++)
			tmp3.add(tmp1[i].getName());
		tmp2.remove(sut.getEscapeCommand());
		assertTrue(tmp3.containsAll(tmp2));
	}
	
	@Test
	public void treatLineWithArgsTest()
	{
		String tmp = "newPrompt";
		
		commandList.changePrompt(tmp);
		replay(commandList);
		this.sut.treatLine("changePrompt " + tmp);
		verify(commandList);
		reset(commandList);
	}
	
	@Test
	public void treatLineExitTest()
	{
		this.sut.treatLine(this.sut.getEscapeCommand());
		assertFalse(shell.isStarted());
	}
	
	@Test
	public void treatLineUnknownCommandTest() throws UnsupportedEncodingException
	{
		String unknownCommand = "unknowncommand";
		String errorMessage = "n-orm: " + unknownCommand + ": command not found";
		
		shell.print(errorMessage);
		replay(shell);
		this.sut.treatLine(unknownCommand);
		verify(shell);
		reset(shell);
	}
	
	@Test
	public void treatLineWrongParametersTest() throws UnsupportedEncodingException
	{
		String command = "changePrompt";
		String args = "newPrompt anotherParameter";
		String errorMessage = "Command format error: " + command + "(java.lang.String)";
		
		shell.print(errorMessage);
		replay(shell);
		this.sut.treatLine(command + " " + args);
		verify(shell);
		reset(shell);
	}
	
	@Test
	public void treatLineWithErrorTest()
	{
		sut.setCommandList(null);
		
		String groovyCommand = "groovy";
		String errorMessage = "n-orm: " + "null" + ": command error";
		
		shell.print(errorMessage);
		replay(shell);
		this.sut.treatLine(groovyCommand);
		verify(shell);
		reset(shell);
		
		sut.setCommandList(commandList);
	}
}