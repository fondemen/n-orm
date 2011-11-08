package com.googlecode.n_orm.f_console.f_shell;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import com.googlecode.n_orm.f_console.commands.CommandList;

public class ShellProcessorTest
{
	private ShellProcessor sut;
	private Shell shell = createMock(Shell.class);
	private CommandList commandProcessor = new CommandList(shell);
	
	@Before
	public void createSut() throws IOException
	{
		this.sut = new ShellProcessor(new Shell());
	}
	
	@Test
	public void testGetCommands()
	{
		Method[] tmp1 = commandProcessor.getClass().getDeclaredMethods();
		List<String> tmp2 = sut.getCommands();
		for (int i = 0; i < tmp1.length; i++)
			assertTrue(tmp2.contains(tmp1[i].getName()));
	}
	
	@Test
	public void testTreatLine()
	{
		this.sut.treatLine("exit");
	}
}