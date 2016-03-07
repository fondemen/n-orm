package com.googlecode.n_orm.console.shell;

import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.console.util.EnumConverter;
import com.googlecode.n_orm.console.util.PackageExplorer;
import com.googlecode.n_orm.query.SearchableClassConstraintBuilder;
import jline.*;
import org.apache.commons.beanutils.ConvertUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

public class Shell {

	public static final String DEFAULT_PROMPT_START = "n-orm";
	public static final String DEFAULT_PROMPT_END = "$ ";
	private static final String[] EMPTY_STRING_ARRAY = new String[0];
	private static List<String> mapClassNames;
	private static Map<Class, EnumConverter> enumConverters;
	private static String lineSeparator;
	private ConsoleReader in;
	private PrintStream out;
	private ShellProcessor shellProcessor;
	private boolean mustStop = true;
	private String prompt;

	public Shell() throws IOException {
		this.out = System.out;
		this.in = new ConsoleReader();
		this.shellProcessor = new ShellProcessor(this);
		this.prompt = DEFAULT_PROMPT_START + "$ ";

		if (mapClassNames == null) {
			enumConverters = new HashMap<Class, EnumConverter>();

			System.out.print("Loading persisting classes from " + PackageExplorer.getLocations() + "...");
			mapClassNames = findAllPersistingClassNames();

			System.out.println();
			System.out.print("Classes Found: Persisting " + mapClassNames.size());
			System.out.print(", Enum " + enumConverters.size());
			System.out.println();
		}

		this.updateProcessorCommands();
	}

	public static String getLineSeparator() {
		if (lineSeparator == null) {
			lineSeparator = System.getProperty("line.separator");

			if ((lineSeparator == null) || (lineSeparator.trim().length() == 0)) {
				lineSeparator = System.lineSeparator();
			}
		}

		return lineSeparator;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static List<String> findAllPersistingClassNames() {
		ArrayList<String> result = new ArrayList<String>();

		for (Class c : PackageExplorer.getClasses("")) {
			if ((c.getAnnotation(Persisting.class) != null))
				result.add(c.getName());

			if (c.isEnum()) {
				EnumConverter converter = new EnumConverter(c);
				enumConverters.put(c, converter);

				ConvertUtils.register(converter, c);
			}
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	public void updateProcessorCommands() {
		// Update the shell processor
		this.shellProcessor.updateProcessorCommands();

		// Get all the commands of the shell processor
		ArrayList<String> methodAsString = new ArrayList<String>();
		ArgumentCompletor argCompletor = null;

		// Try to see if we don't need a Class as a parameter
		List<Method> processorMethods = this.shellProcessor.getCommandsAsMethod();

		for (Method method : processorMethods) {
			if (method != null) {
				if (ShellProcessor.isKeyMethod(method)) {
					Object ctx = this.shellProcessor.getContextElement();

					if (ctx == null)
						continue;

					if (ctx instanceof SearchableClassConstraintBuilder) {
						Class<? extends PersistingElement> clazz = ((SearchableClassConstraintBuilder<?>) ctx).getClazz();

						if (clazz != null) {
							// That should be true
							List<String> keys = new LinkedList<String>();

							for (Field f : KeyManagement.getInstance().detectKeys(clazz)) {
								keys.add(f.getName());
							}

							argCompletor =
									new ArgumentCompletor(
											new SimpleCompletor[] {
													new SimpleCompletor(new String[] { method.getName() }),
													new SimpleCompletor(keys.toArray(EMPTY_STRING_ARRAY))
											});
						}
					}
				}
				else if (method.getParameterTypes().length > 0) {
					for (Class<?> c : method.getParameterTypes()) {
						if (c.equals(Class.class)) {
							// In this case we make an argument completor with the name of all the classes of the project
							argCompletor =
									new ArgumentCompletor(
											new SimpleCompletor[] {
												new SimpleCompletor(new String[] { method.getName() }),
												new SimpleCompletor(mapClassNames.toArray(EMPTY_STRING_ARRAY))
											});
						}
						else if (c.isEnum() && enumConverters.containsKey(c)) {
							argCompletor =
									new ArgumentCompletor(
											new SimpleCompletor[] {
												new SimpleCompletor(new String[] { method.getName() }),
												new SimpleCompletor(enumConverters.get(c).getValues())
											});
						}
						else
							methodAsString.add(method.getName());
					}
				}
				else {
					methodAsString.add(method.getName());
				}
			}
		}

		// Add the shell processor variables if needed
		if (shellProcessor.getContextElement() == null)
			methodAsString.addAll(shellProcessor.getMapShellVariables().keySet());

		// Add the shell processor commands
		methodAsString.add(shellProcessor.getEscapeCommand());
		methodAsString.add(shellProcessor.getZeroCommand());
		methodAsString.add(shellProcessor.getResetCommand());
		if (shellProcessor.getContextElement() != null)
			methodAsString.add(shellProcessor.getUpCommand());
		methodAsString.add(shellProcessor.getShowCommand());

		// To create new persisting objects
		ArgumentCompletor argCompletorNew =
				new ArgumentCompletor(
						new SimpleCompletor[] {
								new SimpleCompletor(new String[]{shellProcessor.getNewCommand()}),
								new SimpleCompletor(Shell.mapClassNames.toArray(EMPTY_STRING_ARRAY))
						});

		// Create the completors
		SimpleCompletor simpleCompletor = new SimpleCompletor(methodAsString.toArray(EMPTY_STRING_ARRAY));
		MultiCompletor multiCompletor = null;
		if (argCompletor == null)
			multiCompletor = new MultiCompletor(new Completor[]{argCompletorNew, simpleCompletor});
		else
			multiCompletor = new MultiCompletor(new Completor[]{argCompletorNew, argCompletor, simpleCompletor});

		// Update jline completor
		Completor[] listCompletor;
		listCompletor = (Completor[]) this.in.getCompletors().toArray(new Completor[0]);
		for (int i = 0; i < listCompletor.length; i++)
			if (this.in.removeCompletor(listCompletor[i]))
				i--;
		this.in.addCompletor(multiCompletor);
	}

	public void doStart() {
		this.mustStop = false;
	}

	public void doStop() {
		this.mustStop = true;
	}

	protected boolean isStarted() {
		return !this.mustStop;
	}

	public void print(String text) {
		out.print(text);
	}

	public void println(String text) {
		print(text + getLineSeparator());
	}

	public void setPrompt(String prompt) {
		this.prompt = prompt;
	}

	public String getCurrentPrompt() {
		return this.prompt;
	}

	protected PrintStream getOutput() {
		return this.out;
	}

	protected void setOutput(PrintStream out) {
		this.out = out;
	}

	protected ConsoleReader getInput() {
		return this.in;
	}

	protected void setInput(ConsoleReader in) {
		this.in = in;
	}

	protected ShellProcessor getShellProcessor() {
		return shellProcessor;
	}

	protected void setShellProcessor(ShellProcessor shellProcessor) {
		this.shellProcessor = shellProcessor;
	}

	public Map<String, Object> getMapCommands() {
		return (this.shellProcessor != null)
				       ? this.shellProcessor.getMapCommands()
				       : null;
	}

	public void setMapCommands(Map<String, Object> mapCommands) {
		if (this.shellProcessor != null)
			this.shellProcessor.setMapCommands(mapCommands);
	}

	public void putEntryMapCommand(String key, Object value) {
		if (this.shellProcessor != null)
			this.shellProcessor.putEntryMapCommand(key, value);
	}

	public void launch() {
		boolean isFirstCommand = true;
		String line;

		this.doStart();
		while (!mustStop) {
			try {
				if (!isFirstCommand)
					this.println("");

				line = this.in.readLine(this.prompt);

				if (line != null) {
					line = line.trim();

					if (line.length() > 0) {
						String[] commands = line.split(";");

						for (String commandLine : commands) {
							shellProcessor.treatLine(commandLine.trim());
						}
					}
				}
				else
					mustStop = true;

				isFirstCommand = false;
			}
			catch (Exception e) {
				println("n-orm: " + e.getMessage() + ": command error");
				e.printStackTrace();
				mustStop = true;
			}
		}
	}
}