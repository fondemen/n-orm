package com.googlecode.n_orm;

import java.util.List;

import com.googlecode.n_orm.operations.Process.ProcessReport;
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
	
	private static String buildMessage(List<Problem> problems, List<Throwable> otherExceptions) {
		StringBuffer ret = new StringBuffer();
		for (Throwable throwable : otherExceptions) {
			ret.append('\n');
			ret.append(throwable.getClass().getName());
			ret.append(": ");
			ret.append(throwable.getMessage());
		}
		for (Problem problem : problems) {
			ret.append('\n');
			ret.append("While treating ");
			PersistingElement elt = problem.getElement();
			if (elt == null) {
				ret.append("unconstructible element with id ");
				ret.append(problem.getRawData().getKey());
			} else
				ret.append(elt);
				
			ret.append(": ");
			Throwable throwable = problem.getReportedProblem();
			ret.append(throwable.getClass().getName());
			ret.append(": ");
			ret.append(throwable.getMessage());
		}
		return ret.toString();
	}
	
	private final Process<? extends PersistingElement> process;
	private final ProcessReport<? extends PersistingElement> report;
	private final List<Problem> problems;
	private final List<Throwable> otherExceptions;
	
	public ProcessException(Process<? extends PersistingElement> p, ProcessReport<? extends PersistingElement> report, List<Problem> problems, List<Throwable> otherExceptions) {
		super("Problem while executing process " + p.getClass().getName() + ':' + buildMessage(problems, otherExceptions), otherExceptions.isEmpty() ? problems.get(0).getReportedProblem() : otherExceptions.get(0));
		this.process = p;
		this.report = report;
		this.problems = problems;
		this.otherExceptions = otherExceptions;
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

	/**
	 * The list of exceptions not linked to a particular element.
	 * @return
	 */
	public List<Throwable> getOtherExceptions() {
		return otherExceptions;
	}
}
