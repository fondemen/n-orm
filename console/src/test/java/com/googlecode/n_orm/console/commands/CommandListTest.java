package com.googlecode.n_orm.console.commands;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import com.googlecode.n_orm.console.shell.Shell;

public class CommandListTest
{
	private CommandList sut;
	private Shell shell = createMock(Shell.class);

	@Before
	public void createSut() throws IOException
	{
		this.sut = new CommandList(shell);
	}
	
	@Test
	public void changePromptTest()
	{
		String newPrompt = "newPrompt$";
		
		shell.setPrompt(newPrompt + " ");
		replay(shell);
		sut.changePrompt(newPrompt);
		verify(shell);
		reset(shell);
	}
	
	@Test
	public void getZeroTest()
	{
		assertEquals(0, sut.getZero());
	}
	
	@Test
	public void getConstantStringTest()
	{
		assertSame("hello world", sut.getConstantString());
	}
}