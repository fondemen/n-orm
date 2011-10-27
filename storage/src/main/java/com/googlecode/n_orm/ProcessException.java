package com.googlecode.n_orm;

import java.util.List;

import com.googlecode.n_orm.query.SearchableClassConstraintBuilder;
import com.googlecode.n_orm.storeapi.Row;

/**
 * Sent while performing {@link StorageManagement#processElements(Class, com.googlecode.n_orm.storeapi.Constraint, Process, int, String[], int, long)} or {@link SearchableClassConstraintBuilder#forEach(Process, int, long)}
 * in case process sent an exception.<br>
 * Message reports the first encountered exception.
 *
 */
public class ProcessException extends Exception {
	private static final long serialVersionUID = 3051299602494010188L;
	
	public static class Problem {
		private final PersistingElement element;
		private final Row rawData;
		private Throwable reportedProblem;
		public Problem(PersistingElement element, Row rawData, Throwable reportedProblem) {
			super();
			this.element = element;
			this.rawData = rawData;
			this.reportedProblem = reportedProblem;
		}
		
		/**
		 * The element that was processed when the problem appeared.
		 * The element can be null in case it could not have been constructed from {@link #getRawData()}.
		 */
		public PersistingElement getElement() {
			return element;
		}
		
		/**
		 * The data retrieved from the store before executing the process from which the {@link #getElement()} should have been constructed.
		 */
		public Row getRawData() {
			return rawData;
		}
		
		/**
		 * The actual problem that appeared while processing {@link #getElement()}.
		 */
		public Throwable getReportedProblem() {
			return reportedProblem;
		}
		
	}
	
	private final Process<? extends PersistingElement> process;
	private final List<Problem> problems;
	
	public ProcessException(Process<? extends PersistingElement> p, List<Problem> problems) {
		super("Problem while executing process " + p.getClass().getName() + ' ' + p, problems.get(0).getReportedProblem());
		this.process = p;
		this.problems = problems;
	}

	/**
	 * The list of exception sent and their corresponding element, that is the element that was under processing while running the process.
	 * @return
	 */
	public List<Problem> getProblems() {
		return problems;
	}

	/**
	 * The process in which the problem(s) appeared.
	 * @return
	 */
	public Process<? extends PersistingElement> getProcess() {
		return process;
	}
}
