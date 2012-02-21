package com.googlecode.n_orm.operations;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.googlecode.n_orm.Callback;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PersistingMixin;
import com.googlecode.n_orm.ProcessCanceller;
import com.googlecode.n_orm.ProcessException;
import com.googlecode.n_orm.TimeoutCanceller;
import com.googlecode.n_orm.ProcessException.Problem;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.storeapi.ActionnableStore;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.Row;
import com.googlecode.n_orm.storeapi.Store;

public class Process {
	public static class ProcessReport<T extends PersistingElement> {
		private int elementsTreated;
		private T lastProcessedElement;
		private Row lastProcessedElementData;
		private Class<T> clazz;
		private Map<String, Field> toBeActivated;
		private long durationInMillis;
		private List<Future<?>> performing;
		
		/**
		 * The number of elements that were processed.
		 */
		public int getElementsTreated() {
			return elementsTreated;
		}
		
		/**
		 * The last element that was processed. Can be null if no element was processed.
		 */
		public T getLastProcessedElement() {
			if (this.lastProcessedElement == null && this.lastProcessedElementData != null) {
				lastProcessedElement = StorageManagement.createElementFromRow(clazz, toBeActivated, lastProcessedElementData);
			}
			return lastProcessedElement;
		}
		
		/**
		 * Total time took for processing elements.
		 */
		public long getDurationInMillis() {
			return durationInMillis;
		}
		
		/**
		 * The list of future for processed that are still performing.
		 * Should be empty if you did not give executor by yourself.
		 * Should be less or equal than the number of admitted threads.
		 */
		public List<Future<?>> getPerforming() {
			Iterator<Future<?>> prfIt = performing.iterator();
			while (prfIt.hasNext()) {
				Future<?> prf = prfIt.next();
				if (prf.isDone())
					prfIt.remove();
			}
			return performing;
		}
		
		/**
		 * Waits for all processes to be done.
		 * Termination is checked every 25 milliseconds.
		 * Should not wait if you did not provide an executor by yourself.
		 * @param timeout number of milliseconds the wait can happen.
		 * @return true if termination happened, false if timeout occured
		 */
		public boolean awaitTermination(long timeout) {
			long end  = System.currentTimeMillis()+timeout;
			while (!getPerforming().isEmpty()) {
				try {
					Thread.sleep(25);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (end < System.currentTimeMillis())
					return false;
			}
			return true;
		}
	}
	
	private static class ProcessRunnable<AE extends PersistingElement, E extends AE> implements Runnable, Serializable {
		private static final long serialVersionUID = 3707496852314499064L;
		
		private final Map<String, Field> toBeActivated;
		private final Row data;
		private final Class<E> clazz;
		private final com.googlecode.n_orm.Process<AE> processAction;
		private final List<Problem> problems;
	
		private ProcessRunnable(Map<String, Field> toBeActivated, Row data,
				Class<E> clazz, com.googlecode.n_orm.Process<AE> processAction,
				List<Problem> problems) {
			this.toBeActivated = toBeActivated;
			this.data = data;
			this.clazz = clazz;
			this.processAction = processAction;
			this.problems = problems;
		}
	
		@Override
		public void run() {
			E elt = null;
			try {
				elt = StorageManagement.createElementFromRow(clazz, toBeActivated, data);
				processAction.process(elt);
			} catch (Throwable t) {
				problems.add(new ProcessException.Problem(elt, data, t));
			}
		}
	}

	private Process() {}

	public static <AE extends PersistingElement, E extends AE> ProcessReport<E> processElements(final Class<E> clazz, Constraint c, final com.googlecode.n_orm.Process<AE> processAction, int limit, String[] families, int threadNumber, ProcessCanceller cancel, ExecutorService executor) throws DatabaseNotReachedException, InterruptedException, ProcessException {
		ProcessReport<E> ret = new ProcessReport<E>();
		long start = System.currentTimeMillis();
		//long end = (threadNumber == 1 || start > Long.MAX_VALUE - timeout) ? Long.MAX_VALUE : start+timeout;
		//final CloseableIterator<E> it = findElement(clazz, c, limit, families);
		Store store = StoreSelector.getInstance().getStoreFor(clazz);
		final Map<String, Field> toBeActivated = families == null ? null : StorageManagement.getAutoActivatedFamilies(clazz, families);
		ret.toBeActivated = toBeActivated;
		ret.clazz = clazz;
		final CloseableKeyIterator keys = store.get(clazz, PersistingMixin.getInstance().getTable(clazz), c, limit, toBeActivated);
		boolean ownsExecutor = executor == null;
		if (ownsExecutor) {
			executor = threadNumber == 1 ? null : Executors.newCachedThreadPool();
		}
		final List<ProcessException.Problem> problems = Collections.synchronizedList(new LinkedList<ProcessException.Problem>());
		List<Throwable> exceptions = new ArrayList<Throwable>();
		try {
			ret.performing = new ArrayList<Future<?>>(threadNumber);
			while (keys.hasNext()) {
				final Row data = keys.next();
				if (cancel != null && cancel.isCancelled())
					throw new InterruptedException(cancel.getErrorMessage(processAction));
				//Cleaning performing from done until there is room for another execution
				while (ret.getPerforming().size() >= threadNumber) {
					Thread.sleep(25); //Hopefully, some execution will be done
					if (cancel != null && cancel.isCancelled())
						throw new InterruptedException(cancel.getErrorMessage(processAction));
				}
				Runnable r = new ProcessRunnable<AE,E>(toBeActivated, data, clazz,
						processAction, problems);
				if (threadNumber == 1)
					r.run();
				else
					ret.performing.add(executor.submit(r));
				ret.lastProcessedElementData = data;
				ret.elementsTreated++;
			}
		} catch (Throwable t) {
			exceptions.add(t);
		} finally {
			keys.close();
			if (executor != null) {
				if (ownsExecutor) {
					executor.shutdown();
					long to = cancel instanceof TimeoutCanceller ? ((TimeoutCanceller)cancel).getDuration() : 60000;
					if (!executor.awaitTermination(to, TimeUnit.MILLISECONDS)) {
						exceptions.add(new InterruptedException("Timeout while expecting termination for process " + processAction.getClass().getName() + ' ' + processAction + " started at " + new Date(start)));
					}
				}
			}
			if (!problems.isEmpty() || !exceptions.isEmpty()) {
				throw new ProcessException(processAction, ret, problems, exceptions);
			}
			ret.durationInMillis = System.currentTimeMillis()-start;
		}
		return ret;
	}

	public static <AE extends PersistingElement, E extends AE> void processElementsRemotely(final Class<E> clazz, final Constraint c, final com.googlecode.n_orm.Process<AE> process, final Callback callback, final int limit, final String[] families, final int threadNumber, final long timeout) throws DatabaseNotReachedException, InstantiationException, IllegalAccessException {
		
		Store store = StoreSelector.getInstance().getStoreFor(clazz);
		if (store instanceof ActionnableStore) {
			Map<String, Field> autoActivatedFamilies = StorageManagement.getAutoActivatedFamilies(clazz, families);
			((ActionnableStore)store).process(PersistingMixin.getInstance().getTable(clazz), c, autoActivatedFamilies, clazz, process, callback);
		} else {
			new Thread() {
				public void run() {
					try {
						processElements(clazz, c, process, limit, families, threadNumber, new TimeoutCanceller(timeout), null);
						if (callback != null)
							callback.processCompleted();
					} catch (Throwable e) {
						if (callback != null)
							callback.processCompletedInError(e);
					}
				}
			}.start();
		}
	}
}
