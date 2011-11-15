package com.googlecode.n_orm.console.commands;

import static org.easymock.EasyMock.*;
import java.io.IOException;
import org.codehaus.groovy.control.CompilationFailedException;
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
	public void exportTest()
	{
		String param = "aParameter";
		
		shell.print("export command called successfully, param=" + param);
		replay(shell);
		sut.export(param);
		verify(shell);
		reset(shell);
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
	public void groovyTest() throws CompilationFailedException
	{
		shell.print("val = 20");
		replay(shell);
		sut.groovy();
		verify(shell);
		reset(shell);
	}
}