package com.googlecode.n_orm.console.shell;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import jline.ConsoleReader;
import org.junit.Before;
import org.junit.Test;
import com.googlecode.n_orm.console.commands.CommandList;

public class ShellTest
{
	private Shell sut;
	private PrintStream out = createMock(PrintStream.class);
	
	@Before
	public void createSut() throws IOException
	{
		this.sut = new Shell();
		this.sut.putEntryMapCommand(CommandList.class.getName(), new CommandList(this.sut));
	}
	
	@Test
	public void accessorsPromptTest()
	{
		String tmp = "newPrompt";
		sut.setPrompt(tmp);
		assertEquals(tmp, sut.getCurrentPrompt());
	}
	
	@Test
	public void accessorsOutputTest()
	{
		PrintStream tmp = createMock(PrintStream.class);
		sut.setOutput(tmp);
		assertEquals(tmp, sut.getOutput());
	}
	
	@Test
	public void accessorsInputTest()
	{
		ConsoleReader tmp = createMock(ConsoleReader.class);
		sut.setInput(tmp);
		assertEquals(tmp, sut.getInput());
	}
	
	@Test
	public void accessorsShellProcessorTest()
	{
		ShellProcessor tmp = createMock(ShellProcessor.class);
		sut.setShellProcessor(tmp);
		assertEquals(tmp, sut.getShellProcessor());
	}
	
	@Test
	public void accessorsMapCommandsTest()
	{
		Map<String, Object> tmp = new HashMap<String, Object>();
		tmp.put("test", new Object());
		sut.setMapCommands(tmp);
		assertEquals(tmp, sut.getMapCommands());
	}
	
	@Test
	public void startStopTest()
	{
		sut.doStart();
		assertTrue(sut.isStarted());
		
		sut.doStop();
		assertFalse(sut.isStarted());
	}
	
	@Test
	public void printTest()
	{
		sut.setOutput(out);
		String tmp = "a text";
		
		out.print(tmp);
		replay(out);
		sut.print(tmp);
		verify(out);
		reset(out);
	}
	
	@Test
	public void printlnTest()
	{
		sut.setOutput(out);
		String tmp = "a text";
		
		out.print(tmp + System.getProperty("line.separator"));
		replay(out);
		sut.println(tmp);
		verify(out);
		reset(out);
	}
	
	@Test
	public void launchTest() throws IOException
	{
		String testString = "groovy" + System.getProperty("line.separator") + "exit";
		InputStream is = new ByteArrayInputStream(testString.getBytes());

		ConsoleReader consoleReader = new ConsoleReader();
		consoleReader.setInput(is);
		
		sut.setInput(consoleReader);
		sut.setOutput(out);

		sut.launch();
		assertFalse(sut.isStarted());
	}
	
	@Test
	public void launchWithErrorTest() throws IOException
	{
		sut.setInput(null);
		sut.setOutput(out);

		String errorMessage = "n-orm: " + "null" + ": command error";
		out.print(errorMessage + System.getProperty("line.separator"));
		replay(out);
		sut.launch();
		verify(out);
		reset(out);
	}
	
	@Test
	public void accessorsMapCommandsWithNullShellProcessorTest()
	{
		sut.setShellProcessor(null);
		Map<String, Object> tmp = new HashMap<String, Object>();
		tmp.put("test", new Object());
		sut.setMapCommands(tmp);
		assertEquals(null, sut.getMapCommands());
	}
}