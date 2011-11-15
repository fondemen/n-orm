package com.googlecode.n_orm.console.shell;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;
import java.io.IOException;
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
	private CommandList commandList = new CommandList(shell);
	
	@Before
	public void createSut() throws IOException
	{
		this.sut = new ShellProcessor(new Shell());
	}
	
	@Test
	public void testGetCommands()
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
	public void testTreatLine()
	{
		this.sut.treatLine("exit");
	}
}