package com.googlecode.n_orm.query;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import com.googlecode.n_orm.Callback;
import com.googlecode.n_orm.CloseableIterator;
import com.googlecode.n_orm.ColumnFamiliyManagement;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.FederatedTableManagement;
import com.googlecode.n_orm.ImplicitActivation;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PersistingElementOverFederatedTable;
import com.googlecode.n_orm.Process;
import com.googlecode.n_orm.ProcessCanceller;
import com.googlecode.n_orm.ProcessException;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.TimeoutCanceller;
import com.googlecode.n_orm.WaitingCallBack;
import com.googlecode.n_orm.operations.ImportExport;
import com.googlecode.n_orm.operations.Process.ProcessReport;
import com.googlecode.n_orm.consoleannotations.Continuator;
import com.googlecode.n_orm.storeapi.ActionnableStore;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.Store;

public class SearchableClassConstraintBuilder<T extends PersistingElement>
		extends ClassConstraintBuilder<T> {

	private Integer limit = null;
	private String [] toBeActivated = null; //null: no activation, non null: autoactivation
	private String tablePostfix = null;


	public SearchableClassConstraintBuilder(Class<T> clazz) {
		super(clazz);
	}

	Integer getLimit() {
		return limit;
	}

	void setLimit(int limit) {
		if (this.limit != null) {
			throw new IllegalArgumentException("A limit is already set to " + this.limit);
		}
		this.limit = limit;
	}

	/**
	 * Returns whether a limit was set for this query.
	 */
	public boolean hasNoLimit() {
		return this.limit == null || this.limit < 1;
	}

	private void checkHasLimits() {
		if (hasNoLimit())
			throw new IllegalStateException("No limit set ; please use withAtMost expression.");
	}

	public String getTablePostfix() {
		return this.tablePostfix;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public Class<T> getClazz() {
		return (Class<T>) super.getClazz();
	}

	@Override
	public KeyConstraintBuilder<T> createKeyBuilder(
			Field f) {
		return new SearchableKeyConstraintBuilder<T>(this, f);
	}
	
	@Continuator
	public LimitConstraintBuilder<T> withAtMost(int limit) {
		return new LimitConstraintBuilder<T>(this, limit);
	}
	
	/**
	 * Sets the table to look for. This is only possible in case searched
	 * element is over a {@link PersistingElementOverFederatedTable federated
	 * table}. Element is thus searched from only one sigle table, whose name is
	 * the original table, postfixed by the given parameter.
	 * 
	 * @param tablePostfix
	 *            the table to look for has to have the given postfix (can be ""
	 *            for original table)
	 * @throws IllegalArgumentException
	 *             in case this class is not over a
	 *             {@link PersistingElementOverFederatedTable federated table}
	 * @throws IllegalArgumentException
	 *             in case this table cannot be part of the federation for
	 *             {@link ConstraintBuilder#ofClass(Class) searched class} (i.e.
	 *             it does not starts with original table name)
	 * @throws IllegalArgumentException
	 *             in case a different table was already set in the query
	 */
	@Continuator
	public SearchableClassConstraintBuilder<T> inTableWithPostfix(String tablePostfix) {
		if (!PersistingElementOverFederatedTable.class.isAssignableFrom(this
				.getClazz()))
			throw new IllegalArgumentException("Class "
					+ this.getClazz().getName()
					+ " is not federated ; cannot assign table in query");

		if (this.tablePostfix != null && !this.tablePostfix.equals(tablePostfix))
			throw new IllegalArgumentException(
					"This query is already limited to table postfix " + this.tablePostfix
							+ " ; cannot set it to " + tablePostfix);

		this.tablePostfix = tablePostfix;
		return this;
	}

	@Override
	public Constraint getConstraint() {
		Constraint ret = super.getConstraint();
		if (this.getTablePostfix() != null)
			ret = new FederatedTableManagement.ConstraintWithPostfix(ret, getTablePostfix());
		return ret;
	}

	/**
	 * Requests for some more family activations while executing the query, in addition to simple properties and families marked as {@link ImplicitActivation}.
	 * @param families the names of the families to be activated (i.e. name of the {@link Map} or {@link Set} property).
	 */
	@Continuator
	public SearchableClassConstraintBuilder<T> andActivate(String... families) {
		if (this.toBeActivated == null)
			this.toBeActivated = families;
		else {
			String [] tba = new String [families.length];
			System.arraycopy(this.toBeActivated, 0, tba, 0, this.toBeActivated.length);
			System.arraycopy(families, 0, tba, this.toBeActivated.length, families.length);
			this.toBeActivated = tba;
		}
		return this;
	}

	/**
	 * Activate all known families for this class (see {@link ConstraintBuilder#ofClass(Class)}).
	 * Please note that only column families for this class (or inherited)
	 * are activated, and not families for subclasses, even if found elements are instance of a subclass for this class.
	 * This remark does not hold for properties, which are all
	 * activated, regardless of the fact they are declared in this class or in a subclass.
	 */
	@Continuator
	public SearchableClassConstraintBuilder<T> andActivateAllFamilies() {
		Set<String> knownCfs = ColumnFamiliyManagement.getInstance().getColumnFamilies(getClazz()).keySet();
		this.toBeActivated = knownCfs.toArray(new String[knownCfs.size()]);
		return this;
	}
	
	/**
	 * Finds the element with the given id.
	 * Any limit set by {@link #withAtMost(int)} will be ignored.
	 * Specified column families are activated if not already (i.e. this element is already known by this thread) (see {@link PersistingElement#activateIfNotAlready(String...)}) ; you should rather perform an {@link PersistingElement#activate(String...)} without specifying any column family with {@link #andActivate(String...)} to force activation.
	 * @param id non printable character string
	 * @return element with given id and class ; null if not found
	 * @throws DatabaseNotReachedException
	 */
	@Continuator
	public T withId(String id) throws DatabaseNotReachedException {
		T ret = StorageManagement.getElement(getClazz(), id);
		if (toBeActivated != null)
			ret.activateIfNotAlready(toBeActivated);
		return ret;
	}
	
	/**
	 * Runs the query to find at most N matching elements. The maximum limit N must be set before using {@link #withAtMost(int)}.
	 * Elements activated (see {@link #andActivate(String...)}), and their keys are all loaded into memory.
	 * @return A (possibly empty) set of elements matching the query limited to the maximum limit.
	 * @throws DatabaseNotReachedException
	 */
	@Continuator
	public NavigableSet<T> go() throws DatabaseNotReachedException {
		checkHasLimits();
		return StorageManagement.findElementsToSet(this.getClazz(), this.getConstraint(), this.limit, this.toBeActivated);
	}
	
	/**
	 * Runs the query to find at most N matching elements. The maximum limit N must be set before using {@link #withAtMost(int)}.
	 * Elements activated (see {@link #andActivate(String...)}). Instead of this function, you should consider using {@link #forEach(Process)}.
	 * @return A (possibly empty) set of elements matching the query limited to the maximum limit, that has to be closed once performed.
	 * @throws DatabaseNotReachedException
	 */
	@Continuator
	public CloseableIterator<T> iterate() throws DatabaseNotReachedException {
		checkHasLimits();
		return StorageManagement.findElement(this.getClazz(), this.getConstraint(), this.limit, this.toBeActivated);
	}

	
	/**
	 * Runs the query to find an element matching the query. Any limit set by {@link #withAtMost(int)} will be ignored (as it is considered to be 1).
	 * The element is activated according to declared column families (see {@link #andActivate(String...)}).
	 * @return A (possibly null) element matching the query.
	 * @throws DatabaseNotReachedException
	 */
	@Continuator
	public T any()  throws DatabaseNotReachedException {
		CloseableIterator<T> found = StorageManagement.findElement(this.getClazz(), this.getConstraint(), 1, this.toBeActivated);
		try {
			if (found.hasNext())
				return found.next();
			else
				return null;
		} finally {
			found.close();
		}
	}
	
	/**
	 * Runs the query to find the number of matching elements.
	 * Any limit set by {@link #withAtMost(int)} will be ignored.
	 */
	@Continuator
	public long count() throws DatabaseNotReachedException {
		return StorageManagement.countElements(this.getClazz(), this.getConstraint());
	}
	
	@Continuator
	public SearchableKeyConstraintBuilder<T> withKey(String key) {
		return (SearchableKeyConstraintBuilder<T>) this.withKeyInt(key);
	}

	@Continuator
	public SearchableKeyConstraintBuilder<T> andWithKey(String key) {
		return this.withKey(key);
	}
	
	/**
	 * Performs an action for each element corresponding to the query.
	 * The maximum limit N must be set before using {@link #withAtMost(int)}.
	 * @param action the action to be performed over each element of the query.
	 * @throws DatabaseNotReachedException
	 * @throws InterruptedException in case threads are interrupted or timeout is reached
	 * @throws ProcessException in case some process sent an exception while running
	 */
	public ProcessReport<T> forEach(Process<T> action) throws DatabaseNotReachedException, InterruptedException, ProcessException {
		return this.forEach(action, 1, 0);
	}
	
	/**
	 * Performs an action for each element corresponding to the query using parallel threads.
	 * The maximum limit N must be set before using {@link #withAtMost(int)}.
	 * Invoking this method is blocking until execution is completed.<br>
	 * In case you only use one thread, process will be performed in this thread.<br>
	 * Be aware that process will not use cache for the current thread, and as such you might need to {@link PersistingElement#activate(String...)} elements stored in the process to see changes.
	 * @param action the action to be performed over each element of the query.
	 * @param threadNumber the maximum number of concurrent threads
	 * @param timeoutMs the max allowed time to execute this task ; useless in case threadNumber is 1
	 * @throws DatabaseNotReachedException
	 * @throws InterruptedException in case threads are interrupted or timeout is reached
	 * @throws ProcessException in case some process sent an exception while running
	 */
	public ProcessReport<T> forEach(Process<T> action, int threadNumber, long timeoutMs) throws DatabaseNotReachedException, InterruptedException, ProcessException {
		return this.forEach(action, threadNumber, timeoutMs, null);
	}
	
	/**
	 * Performs an action for each element corresponding to the query using parallel threads.
	 * The maximum limit N must be set before using {@link #withAtMost(int)}.
	 * Invoking this method is blocking until execution is completed.<br>
	 * In case you only use one thread, process will be performed in this thread.<br>
	 * Be aware that process will not use cache for the current thread, and as such you might need to {@link PersistingElement#activate(String...)} elements stored in the process to see changes.
	 * @param action the action to be performed over each element of the query.
	 * @param threadNumber the maximum number of concurrent threads
	 * @param canceller a canceller object regularly observed while performing request ; in case this object responds <code>false</code> after invoked {@link ProcessCanceller#isCancelled()}, this methods returns a {@link ProcessException} with message found by {@link ProcessCanceller#getErrorMessage(Process)}
	 * @throws DatabaseNotReachedException
	 * @throws InterruptedException in case threads are interrupted or canceler responds <code>false</code> to {@link ProcessCanceller#isCancelled()}
	 * @throws ProcessException in case some process sent an exception while running
	 */
	public ProcessReport<T> forEach(Process<T> action, int threadNumber, ProcessCanceller canceller) throws DatabaseNotReachedException, InterruptedException, ProcessException {
		return this.forEach(action, threadNumber, canceller, null);
	}
	
	/**
	 * Performs an action for each element corresponding to the query using parallel threads ; method might return before process is ended.
	 * The maximum limit N must be set before using {@link #withAtMost(int)}.
	 * Invoking this method can be blocking as long as threadNumber is less that the number of elements to be treated.<br>
	 * Be aware that process will not use cache for the current thread, and as such you might need to {@link PersistingElement#activate(String...)} elements stored in the process to see changes.
	 * @param action the action to be performed over each element of the query.
	 * @param threadNumber the maximum number of concurrent threads
	 * @param timeoutMs the max allowed time to execute this task ; useless in case threadNumber is 1
	 * @param executor the executor to run process ; you need to call {@link ExecutorService#awaitTermination(long, java.util.concurrent.TimeUnit)} to be sure that all elements are processed ; if null, this method is equivalent to {@link #forEach(Process, int, long)}
	 * @throws DatabaseNotReachedException
	 * @throws InterruptedException in case threads are interrupted or timeout is reached
	 * @throws ProcessException in case some process sent an exception while running
	 */
	public ProcessReport<T> forEach(Process<T> action, int threadNumber, long timeoutMs, ExecutorService executor) throws DatabaseNotReachedException, InterruptedException, ProcessException {
		this.checkHasLimits();
		return com.googlecode.n_orm.operations.Process.processElements(this.getClazz(), this.getConstraint(), action, this.limit, this.toBeActivated, threadNumber, new TimeoutCanceller(timeoutMs), executor);
	}
	
	/**
	 * Performs an action for each element corresponding to the query using parallel threads ; method might return before process is ended.
	 * The maximum limit N must be set before using {@link #withAtMost(int)}.
	 * Invoking this method can be blocking as long as threadNumber is less that the number of elements to be treated.<br>
	 * Be aware that process will not use cache for the current thread, and as such you might need to {@link PersistingElement#activate(String...)} elements stored in the process to see changes.
	 * @param action the action to be performed over each element of the query.
	 * @param threadNumber the maximum number of concurrent threads
	 * @param canceller a canceller object regularly observed while performing request ; in case this object responds <code>false</code> after invoked {@link ProcessCanceller#isCancelled()}, this methods returns a {@link ProcessException} with message found by {@link ProcessCanceller#getErrorMessage(Process)}
	 * @param executor the executor to run process ; you need to call {@link ExecutorService#awaitTermination(long, java.util.concurrent.TimeUnit)} to be sure that all elements are processed ; if null, this method is equivalent to {@link #forEach(Process, int, long)}
	 * @throws DatabaseNotReachedException
	 * @throws InterruptedException in case threads are interrupted or canceler responds <code>false</code> to {@link ProcessCanceller#isCancelled()}
	 * @throws ProcessException in case some process sent an exception while running
	 */
	public ProcessReport<T> forEach(Process<T> action, int threadNumber, ProcessCanceller canceller, ExecutorService executor) throws DatabaseNotReachedException, InterruptedException, ProcessException {
		this.checkHasLimits();
		return com.googlecode.n_orm.operations.Process.processElements(this.getClazz(), this.getConstraint(), action, this.limit, this.toBeActivated, threadNumber, canceller, executor);
	}
	
	/**
	 * Performs <i>asynchronously</i> an action for each element corresponding to the query.
	 * In case store for class is <i>not</i> implementing {@link ActionnableStore}, this action is equivalent to an asynchronous call to {@link #forEach(Process)}, and the maximum limit N must be set before using {@link #withAtMost(int)}.
	 * Otherwise, the action is performed directly using the data store server process, and the limit is useless.
	 * In order to wait for process completion, you can use {@link WaitingCallBack}.
	 * @param action class of the action to be performed over each element of the query ; must have a default constructor
	 * @param callBack a callback to be invoked as soon as the process completes ; can be null
	 * @param timeout in case store does not support {@link ActionnableStore}
	 * @throws DatabaseNotReachedException
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public void remoteForEach(Process<T> action, Callback callBack, int threadNumber, long timeout) throws DatabaseNotReachedException, InstantiationException, IllegalAccessException {
		Store s = StoreSelector.getInstance().getStoreFor(this.getClazz());
		if ((!(s instanceof ActionnableStore)) && hasNoLimit())
			throw new IllegalStateException("No limit set while store " + s + " for " + this.getClazz().getName() + " is not implementing " + ActionnableStore.class.getName() + " ; please use withAtMost expression.");
		int limit;
		if (this.limit == null)
			limit = -1;
		else
			limit = this.limit;
		com.googlecode.n_orm.operations.Process.processElementsRemotely(this.getClazz(), this.getConstraint(), action, callBack, limit, this.toBeActivated, threadNumber, timeout);
	}
	
	/**
	 * Runs the query to find at most N matching elements and serialize a representation into the output stream. The maximum limit N can be set before using {@link #withAtMost(int)}, but is not mandatory.
	 * Dependencies are not serialized. Consider carefully setting families to be activated before ; it is advised to use {@link #andActivateAllFamilies()}.
	 * Implementation tries to optimize as much as possible memory impact.
	 * @param out an output stream that must support {@link InputStream#markSupported()} ; note that only one {@link ObjectOutputStream} must be created for a given {@link OutputStream}
	 * @throws IOException 
	 */
	public long exportTo(ObjectOutputStream out) throws IOException, DatabaseNotReachedException {
		if (this.hasNoLimit()) {
			long exported = 0;
			Constraint c = this.getConstraint();
			do {
				com.googlecode.n_orm.operations.ImportExport.ExportReport ex = ImportExport.exportPersistingElements(StorageManagement.findElement(this.getClazz(), c, Integer.MAX_VALUE, this.toBeActivated), out);
				exported+=ex.getExportedElements();
				if (ex.getExportedElements() < Integer.MAX_VALUE)
					return exported;
				else
					c = new Constraint(ex.getElement().getIdentifier()+Character.MIN_VALUE, c.getEndKey());
			} while (true);
		} else {
			return ImportExport.exportPersistingElements(this.iterate(), out).getExportedElements();
		}
	}

	/**
	 * Runs the query to find at most N matching elements and serialize a representation into the output stream. The maximum limit N can be set before using {@link #withAtMost(int)}, but is not mandatory.
	 * Dependencies are not serialized. Consider carefully setting families to be activated before ; it is advised to use {@link #andActivateAllFamilies()}.
	 * Implementation tries to optimize as much as possible memory impact.
	 * @param file the file where elements are to be stored ; overwritten if exists
	 * @throws IOException 
	 */
	@Continuator
	public long exportTo(String file) throws IOException, DatabaseNotReachedException {
		File f = new File(file);
		FileOutputStream fo = new FileOutputStream(f);
		BufferedOutputStream bo = new BufferedOutputStream(fo);
		return this.exportTo(new ObjectOutputStream(bo));
	}
}
