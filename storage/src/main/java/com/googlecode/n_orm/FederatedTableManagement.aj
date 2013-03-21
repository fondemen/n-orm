package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.googlecode.n_orm.query.SearchableClassConstraintBuilder;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Row;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.Store;
import com.googlecode.n_orm.storeapi.ActionnableStore;
import com.googlecode.n_orm.storeapi.DelegatingStore;
import com.googlecode.n_orm.utils.AggregatingIterator;

/**
 * Makes it possible to look for elements of a given class from/to different
 * alternative tables. In the following documentation, "original table" refers
 * to the table in which elements of a given class are stored when not federated
 * (see {@link PersistingMixin#getTable(Class)}). Alternatives are both cached
 * and registered in the data store using table {@link #FEDERATED_META_TABLE}
 * and family {@link #FEDERATED_META_COLUMN_FAMILY} ; key is the name of the
 * original table, and qualifiers are the possible alternatives.
 * 
 * @see Persisting#federated()
 * @see Persisting.FederatedMode
 */
public aspect FederatedTableManagement {

	/**
	 * Table where alternative tables (for a given table) will be stored. The
	 * key is the name of the original table, and columns (in the
	 * {@link #FEDERATED_META_COLUMN_FAMILY} family) the alternatives.
	 */
	public static final String FEDERATED_META_TABLE = "n-orm-federated-tables";
	/**
	 * The column family in which alternative tables are stored.
	 * 
	 * @see #FEDERATED_META_TABLE
	 */
	public static final String FEDERATED_META_COLUMN_FAMILY = "t";

	/**
	 * The time (in ms) during which table alternatives are not loaded again
	 * from the base ; default is 1s
	 */
	public static long TableAlternativeCacheTTLInS = 10000;

	/**
	 * The time (in ms) during which table alternatives are not loaded again
	 * from the base ; default is 1s
	 */
	public static long getTableAlternativeCacheTTLInS() {
		return TableAlternativeCacheTTLInS;
	}

	/**
	 * The time (in s) during which table alternatives are not loaded again from
	 * the base ; default is 1s
	 */
	public static void setTableAlternativeCacheTTLInS(
			long tableAlternativeCacheTTLInS) {
		TableAlternativeCacheTTLInS = tableAlternativeCacheTTLInS;
	}

	private static int ParallelGlobalSearch = 5;

	/**
	 * The maximum number of threads to be used while performing global actions
	 * like a {@link SearchableClassConstraintBuilder#count() counting} or
	 * {@link SearchableClassConstraintBuilder#go() grabbing} elements from a
	 * class.
	 * Default is 5.
	 */
	public static int getParallelGlobalSearch() {
		return ParallelGlobalSearch;
	}

	/**
	 * The maximum number of threads to be used while performing global actions
	 * like a {@link SearchableClassConstraintBuilder#count() counting} or
	 * {@link SearchableClassConstraintBuilder#go() grabbing} elements from a
	 * class.
	 * Default is 5.
	 */
	public static void setParallelGlobalSearch(int parallelGlobalSearch) {
		if (parallelGlobalSearch <= 0)
			throw new IllegalArgumentException("Cannot have less than one global search tasks while attempting to set " + parallelGlobalSearch);
		ParallelGlobalSearch = parallelGlobalSearch;
	}

	// REM: a federated element can only inherit federated elements with similar
	// configuration
	declare parents: (@Persisting(federated!=FederatedMode.NONE) *) implements PersistingElement, PersistingElementOverFederatedTable;

	/**
	 * A place where to register alternatives for an original table. Alternative
	 * tables can be registered or updated from the store using table
	 * {@link FederatedTableManagement#FEDERATED_META_TABLE} and family
	 * {@link FederatedTableManagement#FEDERATED_META_COLUMN_FAMILY}. Updates
	 * occurs at most each
	 * {@link FederatedTableManagement#TableAlternativeCacheTTLInS} seconds.
	 */
	private final static class TableAlternatives {
		/**
		 * The original table
		 */
		private final String mainTable;

		/**
		 * When alternatives for {@link #mainTable} was last updated (epoch in
		 * seconds)
		 */
		private volatile long lastUpdate = -TableAlternativeCacheTTLInS;

		/**
		 * Known table postfixes for {@link #mainTable}
		 */
		private Set<String> postfixes = new TreeSet<String>();
		
		private Boolean hasLegacy = null;
		private volatile long legacyUpdate = 0;
		
		public TableAlternatives(String mainTable) {
			this.mainTable = mainTable;
		}
		
		/**
		 * Returns the store bypassing any cache.
		 */
		private Store getActualStore(Store store) {
			return store instanceof DelegatingStore ? ((DelegatingStore)store).getActualStore() : store;
		}
		
		/**
		 * Whether we believe legacy table (main table) exists
		 */
		public boolean legacyExists(Store store) {
			long now = System.currentTimeMillis();
			if (hasLegacy == null || (legacyUpdate + TableAlternativeCacheTTLInS) < now) {
				this.hasLegacy = store.hasTable(this.mainTable);
				this.legacyUpdate = now;
			}
			return this.hasLegacy;
		}

		/**
		 * Updates alternatives according to meta-informations stored in the
		 * store. An update (for this object) can happen at most each
		 * {@link FederatedTableManagement#TableAlternativeCacheTTLInS} ms.
		 * 
		 * @param store
		 *            where alternative meta-information should be retrieved
		 *            from ; should be the store for a class having
		 *            {@link #mainTable} as original table
		 * @return postfixes that appeared with the update ; empty in case
		 *         tables were not updated from store
		 */
		protected void updateAlternatives(Store store) {
			long now = System.currentTimeMillis();
			if ((this.lastUpdate + TableAlternativeCacheTTLInS) < now) {
				synchronized(this) {
					now = System.currentTimeMillis();
					if ((this.lastUpdate + TableAlternativeCacheTTLInS) < now) {
						// OK, we should update
		
						// Reminding when alternatives were last updated
						this.lastUpdate = now;
						
						//Bypassing any cache
						store = this.getActualStore(store);
		
						// Querying the store (to be found as qualifiers for columns)
						// Table is FEDERATED_META_TABLE
						// key is the original table
						// family is FEDERATED_META_COLUMN_FAMILY
						// obtained cell qualifiers are the stored alternatives.
						Map<String, byte[]> res = store.get(null, FEDERATED_META_TABLE,
								this.mainTable, FEDERATED_META_COLUMN_FAMILY);
						Set<String> newPosts = res == null ? new TreeSet<String>()
								: new TreeSet<String>(res.keySet());
		
						// We should always care about legacy table
						boolean ckeckForLegacyTable = !newPosts.contains("");
		
						// Checking for deleted tables in order to remove them from
						// stored alternatives
						Iterator<String> newAlternativesIt = newPosts.iterator();
						Set<String> deletedPosts = new TreeSet<String>();
						while (newAlternativesIt.hasNext()) {
							String post = newAlternativesIt.next();
							if (!store.hasTable(this.mainTable + post)) {
								newAlternativesIt.remove();
								deletedPosts.add(post);
							}
						}
						// Removing deleted tables from stored alternatives
						if (!deletedPosts.isEmpty()) {
							Map<String, Set<String>> removed = new TreeMap<String, Set<String>>();
							removed.put(FEDERATED_META_COLUMN_FAMILY, deletedPosts);
							store.storeChanges(null, FEDERATED_META_TABLE,
									this.mainTable, null, removed, null);
						}
		
						// Checking for legacy table
						if (ckeckForLegacyTable) {
							if (store.hasTable(mainTable)) {
								this.addPostfix("", store);
								newPosts.add("");
								this.hasLegacy = Boolean.TRUE;
							} else {
								this.hasLegacy = Boolean.FALSE;
							}
						} else {
							this.hasLegacy = Boolean.TRUE;
						}
						this.legacyUpdate = now;
		
						// Recording last state
						this.postfixes = newPosts;
					}
				}
			}
		}

		/**
		 * Registering a new alternative postfix for {@link #mainTable}. In case
		 * this postfix was not known, it is stored in the given data store
		 * 
		 * @param postfix
		 *            the new alternative postfix
		 * @param store
		 *            the store to which register this new alternative ; should
		 *            be the store for a class having {@link #mainTable} as
		 *            original table. null in case no write have ever been done
		 */
		public void addPostfix(String postfix, Store store) {
			if (!this.postfixes.contains(postfix)) {
				synchronized(this) {
					if (this.postfixes.add(postfix)) {
						
						if (store != null) {
							
							// We were not aware of that alternative ;
							// let's register in the store
							
							//Bypassing any cache
							store = this.getActualStore(store);
							
							// Table is FEDERATED_META_TABLE
							// key is the original table
							// family is FEDERATED_META_COLUMN_FAMILY
							// new alternative is the qualifier for an empty cell
							ColumnFamilyData changes = new DefaultColumnFamilyData();
							Map<String, byte[]> change = new TreeMap<String, byte[]>();
							changes.put(FEDERATED_META_COLUMN_FAMILY, change);
							change.put(postfix, null);
							store.storeChanges(null, FEDERATED_META_TABLE, this.mainTable,
									changes, null, null);
						}
					}
				}
			}
		}

		/**
		 * The known alternative postfixes for {@link #mainTable}
		 */
		public Set<String> getPostfixes() {
			return Collections.unmodifiableSet(this.postfixes);
		}
	}

	// Cache for storing table variants (no TTL)
	private static final ConcurrentMap<String /* main table */, TableAlternatives> tablesAlternatives = new ConcurrentHashMap<String, TableAlternatives>();

	/**
	 * The known alternatives for the given original table. Creates the
	 * alternative in cache.
	 */
	private static TableAlternatives getAlternatives(String mainTable) {
		TableAlternatives nw = new TableAlternatives(mainTable);
		TableAlternatives od = tablesAlternatives.putIfAbsent(mainTable, nw);
		return od == null ? nw : od;
	}

	// For test purpose
	static void clearAlternativesCache() {
		tablesAlternatives.clear();
	}

	/**
	 * Adds an alternative to an original table.
	 * 
	 * @param mainTable
	 *            the original table
	 * @param postfix
	 *            the (possibly new) alternative postfix (can be "")
	 * @param store
	 *            the store in which storing alternative table ; should be the
	 *            store for a class having {@link #mainTable} as original table
	 * @see TableAlternatives#addPostfix(String, Store)
	 */
	private static void registerPostfix(String mainTable, String postfix,
			Store store) {
		getAlternatives(mainTable).addPostfix(postfix, store);
	}

	/**
	 * Checks whether a class is stored to a table federation.
	 */
	public static boolean isFederated(Class<? extends PersistingElement> clazz) {
		return clazz != null
				&& PersistingElementOverFederatedTable.class
						.isAssignableFrom(clazz)
				&& clazz.getAnnotation(Persisting.class).federated()
						.isFederated();
	}
	
//	/**
//	 * Class used to perform an operation over necessary table postfixes until goal is reached.
//	 * Operation is retried on other necessary postfixes in case of unconvincing result
//	 * as determined by {@link #isInexistingValue(Object)}.
//	 * First convincing value is sent.
//	 * @param <T> the type of the expected result
//	 */
//	private abstract static class PerformWithRetries<T> {
//		
//		public PerformWithRetries() {}
//		
//		/**
//		 * The operation to perform on a given postfix
//		 */
//		public abstract T perform(String postfix);
//		
//		/**
//		 * Value sent when element on which action is performed does not exist
//		 */
//		public abstract T inexistingValue();
//		
//		/**
//		 * Kind of the performed operation
//		 */
//		public abstract ReadWrite getOperationNature();
//		
//		/**
//		 * Checks whether this value corresponds to a value typical from an inexisting element
//		 */
//		public boolean isInexistingValue(T value) {
//			T inexisting = inexistingValue();
//			return inexisting == null ? value == null : inexisting == value || inexisting.equals(value);
//		}
//		
//		public T performWithRetries(PersistingElementOverFederatedTable self) {
//			boolean locationJustFound; 
//			if (self.tablePostfix == null) {
//				if (self.findTableLocation(this.getOperationNature())) {
//					locationJustFound = true;
//				} else {
//					// We've just found that this element does not exist
//					return inexistingValue();
//				}
//			} else {
//				locationJustFound = false;
//			}
//			
//			LinkedList<String> toBeTested = new LinkedList<String>();
//			// First to be tested is the known localtion
//			toBeTested.addFirst(self.tablePostfix);
//			if (!locationJustFound) {
//				// Adding other location only in case we did not just test location
//				switch(self.getFederatedMode().getConsistency(this.getOperationNature())) {
//				case NONE:
//					// Nothing but known location
//					break;
//				case CONSISTENT_WITH_LEGACY:
//					// Should also check computed postfix and legacy table
//					String computedPostfix = self.getTablePostfix();
//					if (!toBeTested.contains(computedPostfix))
//						toBeTested.addLast(computedPostfix);
//					if (!toBeTested.contains(""))
//						toBeTested.addLast("");
//				case CONSISTENT:
//					// Should check all
//					for (String postfix : self.getPossiblePostfixesWithAnUpdate(self.getStore())) {
//						if (!toBeTested.contains(postfix))
//							toBeTested.addLast(postfix);
//					}
//				}
//			}
//
//			for (String postfix : toBeTested) {
//				T ret = this.perform(postfix);
//				if (!this.isInexistingValue(ret)) {
//					// Great, we found a nice value !
//					// Enforcing proper postfix is registered
//					self.setTablePostfix(postfix, self.getStore());
//					// Returning nice value
//					return ret;
//				}
//			}
//			
//			return this.inexistingValue();
//		}
//	}

	/**
	 * An action that is automatically executed on each postfixes for a given table.
	 * @param <T> the type of the expected result
	 */
	private static abstract class GlobalAction<T> {
		
		private static final ExecutorService executor = new ThreadPoolExecutor(
				0, Integer.MAX_VALUE,
                10L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new ThreadFactory()  {

			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r,"n-orm federated executor");
			}
			
		});
		
		protected GlobalAction() {}

		/**
		 * Runs the query on one possible alternative table
		 */
		protected abstract T localRun(String mainTable, String postfix);

		/**
		 * Merging two results for different alternative table into a single
		 * one. Order in which results are merged is unknown.
		 * 
		 * @param lhs
		 *            either result for the first table or the previously
		 *            aggregated result.
		 */
		protected abstract T add(T lhs, T rhs);
		
		/**
		 * The default value returned by {@link #globalRun(String, Store, Constraint)}
		 * in case table does not exists.
		 */
		protected abstract T emptyValue();

		private Callable<T> createLocalAction(final String mainTable,
				final String postfix, final AtomicInteger running) {
			// Waiting till there are not too many threads executing
			synchronized(running) {
				running.decrementAndGet();
				while (running.get() < 0)
					try {
						running.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			}
			
			return new Callable<T>() {

				@Override
				public T call() throws Exception {
					try {
						
						return localRun(mainTable, postfix);
						
					} finally {
					
						// Letting waiting thread run
						synchronized(running) {
							running.incrementAndGet();
							running.notify();
						}
					}
				}

			};
		}

		/**
		 * Runs {@link #localRun(String) the action} on all referenced
		 * alternative tables (including main table) and
		 * {@link #add(Object, Object) aggregates} results.
		 * Returns null in case no postfix alternative is found. 
		 * 
		 * @param c a constraint where table postfix might have been set (can be null)
		 */
		public T globalRun(String mainTable, Store store, Constraint c) {

			TableAlternatives alts = getAlternatives(mainTable);
			// Making sure we are aware of all possible alternative tables
			alts.updateAlternatives(store);

			if (c != null && (c instanceof ConstraintWithPostfix)) {
				// Table postfix was set in the query
				// computing only for this one 
				String postfix = ((ConstraintWithPostfix) c).getPostfix();
				
				if (!alts.getPostfixes().contains(postfix)) {
					// Unknown postfix...
					if (store.hasTable(mainTable+postfix)) {
						// But still, it exists
						registerPostfix(mainTable, postfix, store);
					} else {
						// Table does not exist ; returning empty value
						return this.emptyValue();
					}
				}
				return this.localRun(mainTable, postfix);
			}
			
			// No need to worry too much in case no postfix exists...
			if (alts.getPostfixes().isEmpty())
				return this.emptyValue();

			AtomicInteger running = new AtomicInteger(getParallelGlobalSearch());
			
			Set<Future<T>> results = new HashSet<Future<T>>();
			for (final String post : alts.getPostfixes()) {
				results.add(executor.submit(this.createLocalAction(mainTable, post, running)));
			}

			// Aggregating results into one single result
			T ret = null;
			for (Future<T> res : results) {
				try {
					ret = ret == null ? res.get() : this.add(ret, res.get());
				} catch (InterruptedException e) {
					throw new DatabaseNotReachedException(e);
				} catch (ExecutionException e) {
					throw new DatabaseNotReachedException(e);
				}
			}

			return ret;
		}
	}

	/**
	 * The postfix for tables ; null if table is not known yet. This persisting
	 * element is actually stored in the table given by
	 * {@link PersistingMixin#getTable(Class)} postfixed with tablePostfix. If
	 * known, alters result for {@link PersistingElement#getTable()};
	 * 
	 * @see Persisting#table()
	 */
	private transient String PersistingElementOverFederatedTable.tablePostfix;

	/**
	 * Sets table postfix as it is discovered.
	 * 
	 * @param postfix
	 * @param store
	 *            null value means that table won't be registered (even not
	 *            cached)
	 * @throws IllegalStateException
	 *             if a different postfix is already known
	 * @throws IllegalStateException
	 *             if {@link Persisting#federated()} stated to check for table
	 *             postfix consistency over time and result for
	 *             {@link PersistingElementOverFederatedTable#getTablePostfix()}
	 *             provides a different result
	 */
	private void PersistingElementOverFederatedTable.setTablePostfix(
			String postfix, Store store) {
		if (postfix == null) {
			this.tablePostfix = null;
			return;
		}

		String oldPostfix = this.tablePostfix;
		this.tablePostfix = postfix;
		this.checkTablePostfixHasNotChanged();
		if (oldPostfix != null && !oldPostfix.equals(this.tablePostfix)) {
			throw new IllegalStateException("Found " + this + " from table "
					+ this.getTable() + this.tablePostfix + " with postfix "
					+ this.tablePostfix + " while another postfix "
					+ oldPostfix + " was registered");
		}
		if (store != null)
			registerPostfix(this.getTable(), this.tablePostfix, store);
	}

	// jut to be sure
	declare error: set(* PersistingElementOverFederatedTable.tablePostfix) && !withincode(private void PersistingElementOverFederatedTable.setTablePostfix(..)) : "Avoid setting this attribute directly ; use setTablePostfix(String postfix, Store store) instead";

	public String PersistingElementOverFederatedTable.getActualTable() {
		if (this.tablePostfix == null)
			return null;
		return this.getTable() + this.tablePostfix;
	}

	private transient FederatedMode PersistingElementOverFederatedTable.federatedMode = null;

	/**
	 * The federated mode for this persisting element.
	 */
	public FederatedMode PersistingElementOverFederatedTable.getFederatedMode() {
		if (this.federatedMode == null) {
			this.federatedMode = this.getClass()
					.getAnnotation(Persisting.class).federated();
		}
		return this.federatedMode;
	}

	/**
	 * Overloads the federated mode for this specific element. You cannot set
	 * the federated mode to {@link FederatedMode#NONE}. This method has only an
	 * impact before actual postfix for the element is found, i.e. before any
	 * operation such as {@link PersistingElement#store()} or
	 * {@link PersistingElement#activate(String...)} is called.
	 * 
	 * @throws IllegalArgumentException
	 *             if mode is set to {@link FederatedMode#NONE}.
	 */
	public void PersistingElementOverFederatedTable.setFederatedMode(
			FederatedMode mode) {
		if (FederatedMode.NONE.equals(mode))
			throw new IllegalArgumentException("Cannot set federated mode to "
					+ mode + " on " + this);
		this.federatedMode = mode;
	}

	/**
	 * The list of tables where to find this object from what we can guess in
	 * order of probability.
	 */
	private List<String> PersistingElementOverFederatedTable.getKnownPossiblePostfixes() {
		List<String> ret = new LinkedList<String>();
		// Table for this object is already known
		if (this.tablePostfix != null) {
			ret.add(this.tablePostfix);
			return ret;
		}

		String mainTable = this.getTable();
		TableAlternatives alternatives = getAlternatives(mainTable);
		Set<String> possibilities = new TreeSet<String>(
				alternatives.getPostfixes());

		// First, asks postfix if the element already knows
		String computedPostfix = this.getTablePostfix();
		ret.add(computedPostfix);
		possibilities.remove(computedPostfix);

		// Otherwise, let's see main table
		if(possibilities.remove("") && !ret.contains("")) {
			ret.add("");
		}

		// And then all other known tables in alphabetical order
		ret.addAll(possibilities);

		return ret;
	}

	/**
	 * The list of tables where to find this object when it failed to be found
	 * from tables given by
	 * {@link PersistingElementOverFederatedTable#getKnownPossibleTables()} ;
	 * only previously unknown tables from from the store are returned by this
	 * function
	 * 
	 * @see TableAlternatives#updateAlternatives(Store)
	 */
	private Collection<String> PersistingElementOverFederatedTable.getPossiblePostfixesWithAnUpdate(
			Store store) {
		TableAlternatives ta = getAlternatives(this.getTable());
		ta.updateAlternatives(store);
		return ta.getPostfixes(); 
	}

	/**
	 * Checks whether
	 * {@link PersistingElementOverFederatedTable#getTablePostfix()} is
	 * consistent with the known table postfix.
	 * 
	 * @throws IllegalStateException
	 *             in case {@link Persisting#federated()} states table postfix
	 *             computation should be checked and is different from known
	 *             postfix
	 * @see FederatedMode#isCheckForChangingPostfix()
	 */
	private void PersistingElementOverFederatedTable.checkTablePostfixHasNotChanged() {
		FederatedMode fm = this.getFederatedMode();
		if (this.tablePostfix != null && fm.isCheckForChangingPostfix()) {
			// Let's check whether a new computation for table postfix changes
			// its value...
			String computedPostfix = this.getTablePostfix();
			if (!this.tablePostfix.equals(computedPostfix)) {
				// Could still be forgiven in legacy mode
				if (this.tablePostfix.length() != 0)
					throw new IllegalStateException(this
							+ " already registered in table " + this.getTable()
							+ " with postfix " + this.tablePostfix
							+ " while computed postfix states now "
							+ computedPostfix);
			}
		}
	}
	
	/**
	 * Creates a temporary element of the given class, for the given table postfix.
	 * Element is removed from the cache.
	 */
	private static PersistingElementOverFederatedTableWithMerge createTemporaryElement(
			Class<? extends PersistingElementOverFederatedTableWithMerge> clazz,
			Store store, String id, String postfix, Set<String> families, ColumnFamilyData values) {
		KeyManagement km = KeyManagement.getInstance();
		// Removing any cached element so that created one is really new
		km.unregister(clazz, id);
		
		// Creating element
		PersistingElementOverFederatedTableWithMerge elt =
				(PersistingElementOverFederatedTableWithMerge)km.createElement(clazz, id);
		// Not keeping element in cache (it's temporary)
		km.unregister(elt);
		if (families != null) {
			// In case we already know some information
			elt.activateFromRawData(families, values);
		}
		// Table postfix
		elt.setTablePostfix(postfix, store);
		
		return elt;
	}
	
	/**
	 * Tries to repair an inconsistency by merging the given element into this element.
	 * In case of success, this element is stored and the given element deleted. 
	 */
	private void PersistingElementOverFederatedTableWithMerge.repairInconsistencyByMerging(
			PersistingElementOverFederatedTableWithMerge elt) {
		assert this != elt;
		assert this.getClass().equals(elt.getClass());
		assert this.tablePostfix != null;
		assert elt.tablePostfix != null;
		assert !this.tablePostfix.equals(elt.tablePostfix);

		KeyManagement km = KeyManagement.getInstance();
		// There is no official element for this id yet (as there is an inconsistency)
		km.unregister(this);
		
		// Trying to repair by merging
		try {
			this.mergeWith(elt);
		} catch (Exception x) {
			// Couldn't merge :(
			throw new IllegalStateException(
					"Found unmergeable duplicate data with id " + this.getIdentifier()
					+ " in tables '" + this.getTable()
					+ " with postfixes " + this.tablePostfix
					+ "' and '" + elt.tablePostfix + '\'', x);
		}
		
		// Immediately storing this and deleting other element once merged
		this.store();
		// Can't delete using elt.delete()
		// "this" exists and makes elt believe that it still exists after delete (assertion fails)
		this.getStore().delete(
				new MetaInformation().forElement(elt).withPostfixedTable(elt.getTable(), elt.tablePostfix),
				elt.getActualTable(), elt.getIdentifier());
		// Consistency issue repaired :)
		
		// this is now the officiel version for this id
		km.register(this);
	}

	/**
	 * Sets the table postfix in case it is not known by searching this
	 * element's identifer across all possible tables. Tables are explored in
	 * order of probability: with computed table postfix, with no postfix, with
	 * already known postfixes for the original table, will all possible
	 * postfixes (not already tested) taken from the store (see
	 * {@link TableAlternatives#updateAlternatives(Store)}).
	 * 
	 * @return whether this table was newly found
	 */
	private boolean PersistingElementOverFederatedTable.findTableLocation(
			final ReadWrite mode) {
		if (this.tablePostfix != null) {
			this.checkTablePostfixHasNotChanged();
			return false;
		}
		String computedPostfix = this.getTablePostfix();
		Consistency consistencyLevel = this.getFederatedMode().getConsistency(mode);

		final Store store = this.getStore();

		final String mainTable = this.getTable();
		final String id = this.getIdentifier();
		switch (consistencyLevel) {
		case NONE:
			// Trusting computed value
			this.setTablePostfix(computedPostfix, mode.isRead() ? null : store);
			return true;
			
		case CONSISTENT_WITH_LEGACY:
			// Soft consistency ; only testing expected table and legacy

			// First trying with expected table
			if (this.testTableLocation(mainTable, computedPostfix, id, store))
				return true;

			// Then trying with legacy table (if different)
			if (!"".equals(computedPostfix)
				&& FederatedTableManagement.getAlternatives(this.getTable()).legacyExists(store)
				&& this.testTableLocation(mainTable, "", id, store))
				return true;
			
			break;
			
		case CONSISTENT:
			
			Set<String> tested = new HashSet<String>();
			for (String post : this.getKnownPossiblePostfixes()) {
				if (tested.add(post) && this.testTableLocation(mainTable, post, id, store))
					return true;
			}
			// No found yet ; querying possible alternatives from store
			for (String post : this.getPossiblePostfixesWithAnUpdate(store)) {
				if (tested.add(post) && this.testTableLocation(mainTable, post, id, store))
					return true;
			}
			
//			// Hard consistency ; checking all possible tables in parallel
//			GlobalAction<Set<String>> tableSearch = new GlobalAction<Set<String>>() {
//				
//				@Override
//				protected Set<String> localRun(String mainTable, String postfix) {
//					return store.exists(null, mainTable	+ postfix, id) ? 
//								Collections.singleton(postfix)
//							: 	Collections.<String>emptySet();
//				}
//				
//				@Override
//				protected Set<String> emptyValue() {
//					return Collections.emptySet();
//				}
//
//				@Override
//				protected Set<String> add(Set<String> lhs, Set<String> rhs) {
//					// Using a tree set so that found postfixes are sorted in alphabetica order
//					// This makes all processes result in the same and predictible way
//					if (!(lhs instanceof TreeSet))
//						lhs = new TreeSet<String>(lhs);
//					lhs.addAll(rhs);
//					return lhs;
//				}
//				
//			};
//			
//			Set<String> found = tableSearch.globalRun(mainTable, store, null);
//			if (found.size() > 1) {
//				// Inconsistency detected ; repairing
//				
//				// Repair possible only if class for this object makes it possible
//				if (! (this instanceof PersistingElementOverFederatedTableWithMerge))
//					throw new DatabaseNotReachedException(
//							"Inconsistency detected: found element " + this
//							+ " in tables " + mainTable + " with the following postfixes " + found
//							+ " ; make " + this.getClass().getName()
//							+ " implement " + PersistingElementOverFederatedTableWithMerge.class.getName()
//							+ " in order to recover");
//				
//				// Postfix of the final element
//				String targetPostfix;
//				if (found.remove(computedPostfix)) {
//					targetPostfix = computedPostfix;
//				} else {
//					// Taking the first postfix in alphabetical order
//					// so that any process will result in the same target postfix
//					targetPostfix = ((NavigableSet<String>)found).first();
//				}
//				this.setTablePostfix(targetPostfix, store);
//
//				KeyManagement km = KeyManagement.getInstance();
//				// Forgetting about this element in the cache
//				km.unregister(this);
//				
//				// Merging other elements into this object
//				for (String postfix : found) {
//					PersistingElementOverFederatedTableWithMerge elt =
//							FederatedTableManagement.createTemporaryElement(
//									this.getClass().asSubclass(PersistingElementOverFederatedTableWithMerge.class),
//									this.getStore(),
//									this.getIdentifier(),
//									postfix,
//									null, null);
//					((PersistingElementOverFederatedTableWithMerge)this).repairInconsistencyByMerging(elt);
//				}
//				
//				km.register(this);
//				
//				assert tableSearch.globalRun(mainTable, store, null).size() == 1;
//				
//				return true;
//				
//			} else if (found.size() == 1) {
//				this.setTablePostfix(found.iterator().next(), store);
//				return true;
//			}
			
			break;
			
		}

		// Still not found ; setting postfix to computed value
		// Only registering in case we are sure table exists (or about to)
		this.setTablePostfix(computedPostfix, mode.isRead() ? null : store);
		return false;
	}

	private boolean PersistingElementOverFederatedTable.testTableLocation(
		String mainTable, String postfix, String id, Store store) {
		if (store.exists(new MetaInformation().forElement(this)
				.withPostfixedTable(mainTable, postfix), mainTable
				+ postfix, id)) {
			this.setTablePostfix(postfix, store);
			return true;
		}
		return false;
	}

	// getTablePostfix might return null ; we'll consider it is equivalent to
	// empty postfix
	String around():
		call(String PersistingElementOverFederatedTable+.getTablePostfix())
		&& within(FederatedTableManagement) {
		String ret = proceed();
		return ret == null ? "" : ret;
	}

	// Generic pointcut to state where things have to be woven
	pointcut inNOrm(): 
		within(com.googlecode.n_orm..*) && !within(*..*Test) && !within(FederatedTableManagement) && !within(DelegatingStore+);

	// ===================================
	// element-level operations
	// ===================================

	// Store
	void around(MetaInformation meta, String table, Store store):
		call(void Store+.storeChanges(..))
		&& inNOrm()
		&& target(store)
		&& args(meta, table, ..)
		&& if(meta != null && meta.getElement() instanceof PersistingElementOverFederatedTable) {
		PersistingElementOverFederatedTable self = (PersistingElementOverFederatedTable) meta
				.getElement();
		self.findTableLocation(ReadWrite.WRITE);
		registerPostfix(table, self.tablePostfix, store);
		proceed(meta.withPostfixedTable(table, self.tablePostfix), table
				+ self.tablePostfix, store);
	}

	// Activate
	ColumnFamilyData around(final MetaInformation meta, final String table, final String id, final Set<String> families, final Store store):
		call(ColumnFamilyData Store.get(MetaInformation,String,String,Set<String>))
		&& inNOrm()
		&& args(meta, table, id, families)
		&& target(store)
		&& if(meta != null && meta.getElement() instanceof PersistingElementOverFederatedTable) {
		PersistingElementOverFederatedTable self = (PersistingElementOverFederatedTable) meta
				.getElement();
		
//		// Testing possible tables depending on consistency
//		return new PerformWithRetries<ColumnFamilyData>() {
//
//			@Override
//			public ColumnFamilyData perform(String postfix) {
//				return store.get(meta.withPostfixedTable(table, postfix), table+postfix, id, families);
//			}
//
//			@Override
//			public ColumnFamilyData inexistingValue() {
//				return null;
//			}
//
//			@Override
//			public ReadWrite getOperationNature() {
//				return ReadWrite.READ;
//			}
//		}.performWithRetries(self);
			
		if (self.tablePostfix == null
				&& !self.findTableLocation(ReadWrite.READ))
			// We've just found that this element does not exist
			return null;
		return proceed(meta.withPostfixedTable(table, self.tablePostfix), table
				+ self.tablePostfix, id, families, store);
	}

	// Exists
	boolean around(final MetaInformation meta, final String table, final String id, final Store store):
		call(boolean Store.exists(MetaInformation, String, String))
		&& inNOrm()
		&& args(meta, table, id)
		&& target(store)
		&& if(meta != null && meta.getElement() instanceof PersistingElementOverFederatedTable) {
		PersistingElementOverFederatedTable self = (PersistingElementOverFederatedTable) meta
				.getElement();
		
//		return new PerformWithRetries<Boolean>() {
//
//			@Override
//			public Boolean perform(String postfix) {
//				return store.exists(meta.withPostfixedTable(table, postfix), table+postfix, id);
//			}
//
//			@Override
//			public Boolean inexistingValue() {
//				return false;
//			}
//
//			@Override
//			public ReadWrite getOperationNature() {
//				return ReadWrite.READ;
//			}
//		}.performWithRetries(self);
		
		if (self.tablePostfix == null
				&& !self.findTableLocation(ReadWrite.READ))
			// We've just found that this element does not exist
			return false;
		return proceed(meta.withPostfixedTable(table, self.tablePostfix), table
				+ self.tablePostfix, id, store);
	}

	// Delete
	void around(final MetaInformation meta, final String table, final String id, final Store store):
		call(void Store+.delete(..))
		&& inNOrm()
		&& target(store)
		&& args(meta, table, id)
		&& if(meta != null && meta.getElement() instanceof PersistingElementOverFederatedTable) {
		final PersistingElementOverFederatedTable self = (PersistingElementOverFederatedTable) meta
				.getElement();
		
//		new PerformWithRetries<Void>() {
//
//			@Override
//			public Void perform(String postfix) {
//				store.delete(meta.withPostfixedTable(table, postfix), table+postfix, id);
//				return null;
//			}
//
//			@Override
//			public Void inexistingValue() {
//				// Also forces performing on all necessary tables
//				return null;
//			}
//			
//			@Override
//			public ReadWrite getOperationNature() {
//				return ReadWrite.READ_OR_WRITE;
//			}
//		}.performWithRetries(self);
//		self.setTablePostfix(null, null);
		
		self.findTableLocation(ReadWrite.READ_OR_WRITE);
		proceed(meta.withPostfixedTable(table, self.tablePostfix), table
				+ self.tablePostfix, id, store);
		self.setTablePostfix(null, null);

//		// Checking other tables, depending on consistency level
//		while(self.findTableLocation(ReadWrite.READ_OR_WRITE)) {
//			// Found element in yet another table ; deleting 
//			store.delete(meta.withPostfixedTable(table, self.tablePostfix),
//					self.getActualTable(), id);
//			self.setTablePostfix(null, null);
//		}
	}

	// ===================================
	// family-level operations
	// ===================================

	// Exists
	boolean around(MetaInformation meta, String table):
		call(boolean Store.exists(MetaInformation, String, String, String))
		&& inNOrm()
		&& args(meta, table, ..)
		&& if(meta != null && meta.getElement() instanceof PersistingElementOverFederatedTable) {
		PersistingElementOverFederatedTable self = (PersistingElementOverFederatedTable) meta
				.getElement();
		if (self.tablePostfix == null
				&& !self.findTableLocation(ReadWrite.READ))
			// We've just found that this element does not exist
			return false;
		return proceed(meta.withPostfixedTable(table, self.tablePostfix), table
				+ self.tablePostfix);
	}

	// Get column
	byte[] around(MetaInformation meta, String table):
		call(byte[] Store.get(MetaInformation, String, String, String, String))
		&& inNOrm()
		&& args(meta, table, ..)
		&& if(meta != null && meta.getElement() instanceof PersistingElementOverFederatedTable) {
		PersistingElementOverFederatedTable self = (PersistingElementOverFederatedTable) meta
				.getElement();
		if (self.tablePostfix == null
				&& !self.findTableLocation(ReadWrite.READ))
			// We've just found that this element does not exist
			return null;
		return proceed(meta.withPostfixedTable(table, self.tablePostfix), table
				+ self.tablePostfix);
	}

	// Get all columns
	Map<String, byte[]> around(MetaInformation meta, String table):
		(		call(Map<String, byte[]> Store.get(MetaInformation, String, String, String))
			||	call(Map<String, byte[]> Store.get(MetaInformation, String, String, String, Constraint))
		)
		&& inNOrm()
		&& args(meta, table, ..)
		&& if(meta != null && meta.getElement() instanceof PersistingElementOverFederatedTable) {
		PersistingElementOverFederatedTable self = (PersistingElementOverFederatedTable) meta
				.getElement();
		if (self.tablePostfix == null
				&& !self.findTableLocation(ReadWrite.READ))
			// We've just found that this element does not exist
			return null;
		return proceed(meta.withPostfixedTable(table, self.tablePostfix), table
				+ self.tablePostfix);
	}

	// ===================================
	// global-level operations
	// ===================================

	// Count
	long around(final MetaInformation meta, final String table,
			final Constraint c, final Store store):
		call(long Store.count(MetaInformation, String, Constraint))
		&& inNOrm()
		&& target(store)
		&& args(meta, table, c) {
		Class<? extends PersistingElement> clazz = meta == null ? null : meta
				.getClazzNoCheck();
		if (!isFederated(clazz)) {
			return proceed(meta, table, c, store);
		}

		return new GlobalAction<Long>() {

			@Override
			protected Long localRun(String mainTable, String postfix) {
				return store.count(new MetaInformation(meta)
						.withPostfixedTable(mainTable, postfix), mainTable
						+ postfix, c);
			}
			
			@Override
			protected Long emptyValue() {
				return 0l;
			}

			@Override
			protected Long add(Long lhs, Long rhs) {
				return lhs + rhs;
			}
		}.globalRun(table, store, c);
	}

	/**
	 * A {@link Row} decorated with the table from which data was retrieved.
	 */
	private static class RowWithTable implements Row {
		private final String mainTable, tablePostfix;
		private final Row row;

		public RowWithTable(String mainTable, String tablePostfix, Row row) {
			super();
			this.mainTable = mainTable;
			this.tablePostfix = tablePostfix;
			this.row = row;
		}

		public String getMainTable() {
			return mainTable;
		}

		public String getTablePostfix() {
			return tablePostfix;
		}

		@Override
		public String getKey() {
			return row.getKey();
		}

		@Override
		public ColumnFamilyData getValues() {
			return row.getValues();
		}
	}

	/**
	 * A {@link CloseableIterator} decorated with the table from which keys are
	 * found. {@link #next() Returns} instances of {@link RowWithTable}.
	 */
	private static class CloseableKeyIteratorWithTable implements
			CloseableKeyIterator {
		private final String mainTable, tablePostfix;
		private final CloseableKeyIterator iterator;

		public CloseableKeyIteratorWithTable(String mainTable,
				String tablePostfix, CloseableKeyIterator iterator) {
			super();
			this.mainTable = mainTable;
			this.tablePostfix = tablePostfix;
			this.iterator = iterator;
		}

		public String getMainTable() {
			return mainTable;
		}

		public String getTablePostfix() {
			return tablePostfix;
		}

		@Override
		public void close() {
			iterator.close();
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		/**
		 * @return an instance of {@link RowWithTable}
		 */
		@Override
		public Row next() {
			Row r = iterator.next();
			return r == null ? null : new RowWithTable(this.getMainTable(),
					getTablePostfix(), r);
		}

		@Override
		public void remove() {
			iterator.remove();
		}
		
		@Override
		public String toString() {
			return "iterator on table '" + this.getMainTable() + "' with postfix '" + this.getTablePostfix() +'\'';
		}

	}

	// Search
	CloseableKeyIterator around(final MetaInformation meta, final String table,
			final Constraint c, final int limit, final Set<String> families,
			final Store store):
		call(CloseableKeyIterator Store.get(MetaInformation, String, Constraint,int, Set<String>))
		&& inNOrm()
		&& target(store)
		&& args(meta, table, c, limit, families) {
		final Class<? extends PersistingElement> clazz = meta == null ? null : meta
				.getClazzNoCheck();
		if (!isFederated(clazz)) {
			return proceed(meta, table, c, limit, families, store);
		}

		return new GlobalAction<CloseableKeyIterator>() {

			@Override
			protected CloseableKeyIterator localRun(String mainTable,
					String postfix) {
				return new CloseableKeyIteratorWithTable(mainTable, postfix,
						store.get(new MetaInformation(meta).withPostfixedTable(
								mainTable, postfix), mainTable + postfix, c,
								limit, families));
			}
			
			@Override
			protected CloseableKeyIterator emptyValue() {
				return new EmptyCloseableIterator();
			}

			@Override
			protected CloseableKeyIterator add(CloseableKeyIterator lhs,
					CloseableKeyIterator rhs) {
				if (lhs == null)
					return rhs;
				if (!(lhs instanceof AggregatingIterator)) {
					
					AggregatingIterator ret =
							PersistingElementOverFederatedTableWithMerge.class.isAssignableFrom(clazz) ?
									// Aggregating iterator able to repair inconsistencies
								new AggregatingIterator() {
									@Override
									public Row merge(Row r1, CloseableKeyIterator it1, Row r2, CloseableKeyIterator it2) throws Exception {
										// Inconsistency detected, trying to repair
										assert r1 instanceof RowWithTable;
										assert r2 instanceof RowWithTable;
										assert r1.getKey().equals(r2.getKey());
										assert table.equals(((RowWithTable)r1).getMainTable());
										assert table.equals(((RowWithTable)r2).getMainTable());
										String post1 = ((CloseableKeyIteratorWithTable)it1).getTablePostfix();
										String post2 = ((CloseableKeyIteratorWithTable)it2).getTablePostfix();
										assert post1 != null;
										assert post2 != null;
										assert !post1.equals(post2);
										KeyManagement km = KeyManagement.getInstance();
										
										// Creating elements from r1 and r2
										PersistingElementOverFederatedTableWithMerge elt1 =
												FederatedTableManagement.createTemporaryElement(
														clazz.asSubclass(PersistingElementOverFederatedTableWithMerge.class),
														store, r1.getKey(), post1,
														families, r1.getValues());
										PersistingElementOverFederatedTableWithMerge elt2 =
												FederatedTableManagement.createTemporaryElement(
														clazz.asSubclass(PersistingElementOverFederatedTableWithMerge.class),
														store, r2.getKey(), post2,
														families, r2.getValues());
										km.unregister(elt1);
										km.unregister(elt2);
										
										// Checking hoped location from 1
										String expectedTablePostFix = elt1.getTablePostfix();
										if (post2.equals(expectedTablePostFix)) {
											// Swapping 1 and 2 as 2 is on the right place, even in 1's belief
											PersistingElementOverFederatedTableWithMerge ptmp = elt2;
											elt2 = elt1;
											elt1 = ptmp;
											Row rtmp = r2;
											r2 = r1;
											r1 = rtmp;
											String stmp = post2;
											post2 = post1;
											post1 = stmp;
										}
										
										// Actual repair
										elt1.repairInconsistencyByMerging(elt2);
										
										// Preparing row to be returned (could eventually be activated to elt1)
										String post = post1;
										final String id = r1.getKey();
										// Expected data ; empty if no family expected, otherwise to be grabbed from the (actual) store
										final ColumnFamilyData data = families == null ? new DefaultColumnFamilyData() : store.get(meta, table+post, id, families);
										return new RowWithTable(table, post,
												new Row() {
													
													@Override
													public ColumnFamilyData getValues() {
														return data;
													}
													
													@Override
													public String getKey() {
														return id;
													}
												});
									}
								}
						: new AggregatingIterator() {
									@Override
									public Row merge(Row r1, CloseableKeyIterator it1, Row r2, CloseableKeyIterator it2) throws Exception {
										try {
											// Should throw an exception with a nice message
											return super.merge(r1, it1, r2, it2);
										} catch (Exception x) {
											// Asking for an PersistingElementOverFederatedTableWithMerge implementation
											throw new DatabaseNotReachedException(
													"Inconsistency detected on row " + r1.getKey()
													+ " ; make " + clazz.getName()
													+ " implement " + PersistingElementOverFederatedTableWithMerge.class.getName()
													+ " to repair it", x);
										}
									}
									
								};
					ret.addIterator(lhs);
					lhs = ret;
				}
				((AggregatingIterator) lhs).addIterator(rhs);
				return lhs;
			}
		}.globalRun(table, store, c);
	}

	// When creating an element from a row using a search, let's immediately set
	// its table
	after(RowWithTable row) returning (PersistingElementOverFederatedTable self) : 
		execution(PersistingElement createElementFromRow(Class, Map<String, Field>, Row)) 
		&& args(.., row){
		if (self != null) {
			self.setTablePostfix(row.getTablePostfix(), self.getStore());
		}
	}

	// Remote process
	void around(final MetaInformation meta, final String table,
			final Constraint c, final Set<String> families,
			final Class<? extends PersistingElement> element,
			final Process<? extends PersistingElement> action,
			final Callback callback, final ActionnableStore store):
		call(void ActionnableStore.process(MetaInformation, String, Constraint, Set<String>, Class, Process, Callback))
		&& inNOrm()
		&& target(store)
		&& args(meta, table, c, families, element, action, callback) {
		Class<? extends PersistingElement> clazz = meta == null ? null : meta
				.getClazzNoCheck();
		if (!isFederated(clazz)) {
			proceed(meta, table, c, families, element, action, callback, store);
			return;
		}

		new GlobalAction<Void>() {

			@SuppressWarnings("unchecked")
			@Override
			protected Void localRun(String mainTable, String postfix) {
				store.process(new MetaInformation(meta).withPostfixedTable(
						mainTable, postfix), mainTable + postfix, c, families,
						element, (Process<PersistingElement>) action, callback);
				return null;
			}
			
			@Override
			protected Void emptyValue() {
				return null;
			}

			@Override
			protected Void add(Void lhs, Void rhs) {
				return null;
			}
		}.globalRun(table, store, c);
	}

	// We're using Constraint to transmit searched table
	public static class ConstraintWithPostfix extends Constraint {
		private final String postfix;
		private final Constraint constraint;

		public ConstraintWithPostfix(Constraint c, String postfix) {
			super(c == null ? null : c.getStartKey(), c == null ? null : c
					.getEndKey());
			this.constraint = c;
			this.postfix = postfix;
		}

		public String getPostfix() {
			return this.postfix;
		}

		public Constraint getConstraint() {
			return this.constraint;
		}
	}
}
