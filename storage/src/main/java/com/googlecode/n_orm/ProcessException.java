package com.googlecode.n_orm;

import java.util.Map;

import com.googlecode.n_orm.query.SearchableClassConstraintBuilder;

/**
 * Sent while performing {@link StorageManagement#processElements(Class, com.googlecode.n_orm.storeapi.Constraint, Process, int, String[], int, long)} or {@link SearchableClassConstraintBuilder#forEach(Process, int, long)}
 * in case process sent an exception.<br>
 * Message reports the first encountered exception.
 *
 */
public class ProcessException extends Exception {
	private static final long serialVersionUID = 3051299602494010188L;
	
	private final Map<? extends PersistingElement, Throwable> problems;
	
	public <AE extends PersistingElement, E extends AE> ProcessException(Process<AE> p, Map<E, Throwable> problems) {
		super("Problem while executing process " + p.getClass().getName(), problems.entrySet().iterator().next().getValue());
		this.problems = problems;
	}

	/**
	 * The list of exception sent and their corresponding element, that is the element that was under processing while running the process.
	 * @return
	 */
	public Map<? extends PersistingElement, Throwable> getProblems() {
		return problems;
	}
}
