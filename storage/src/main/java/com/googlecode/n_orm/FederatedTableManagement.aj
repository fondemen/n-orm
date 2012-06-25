package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.googlecode.n_orm.FederatedMode.Consistency;
import com.googlecode.n_orm.FederatedMode.ReadWrite;
import com.googlecode.n_orm.query.SearchableClassConstraintBuilder;
import com.googlecode.n_orm.storeapi.CloseableKeyIterator;
import com.googlecode.n_orm.storeapi.Constraint;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.Row;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.Store;
import com.googlecode.n_orm.query.ClassConstraintBuilder;

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

	public static int ParallelGlobalSearch = 5;

	/**
	 * The maximum number of threads to be used while performing global actions
	 * like a {@link SearchableClassConstraintBuilder#count() counting} or
	 * {@link SearchableClassConstraintBuilder#go() grabbing} elements from a
	 * class.
	 */
	public static int getParallelglobalsearch() {
		return ParallelGlobalSearch;
	}

	/**
	 * The maximum number of threads to be used while performing global actions
	 * like a {@link SearchableClassConstraintBuilder#count() counting} or
	 * {@link SearchableClassConstraintBuilder#go() grabbing} elements from a
	 * class.
	 */
	public static int getParallelGlobalSearch() {
		return ParallelGlobalSearch;
	}

	public static void setParallelGlobalSearch(int parallelGlobalSearch) {
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
		private long lastUpdate = -TableAlternativeCacheTTLInS;

		/**
		 * Known table alternatives for {@link #mainTable}
		 */
		private Set<String> alternatives = new TreeSet<String>();

		public TableAlternatives(String mainTable) {
			this.mainTable = mainTable;
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
		 * @return tables that appeared with the update ; empty in case tables
		 *         were not upated from store
		 */
		protected synchronized Set<String> updateAlternatives(Store store) {
			long now = System.currentTimeMillis();
			if ((this.lastUpdate + TableAlternativeCacheTTLInS) < now) {
				// OK, we should update

				// Reminding when alternatives were last updated
				this.lastUpdate = now;

				// Querying the store (to be found as qualifiers for columns)
				// Table is FEDERATED_META_TABLE
				// key is the original table
				// family is FEDERATED_META_COLUMN_FAMILY
				// obtained cell qualifiers are the stored alternatives.
				Map<String, byte[]> res = store.get(null, null,
						FEDERATED_META_TABLE, this.mainTable,
						FEDERATED_META_COLUMN_FAMILY);
				Set<String> newAlternatives = res == null ? new TreeSet<String>()
						: new TreeSet<String>(res.keySet());

				// Checking for deleted tables in order to remove them from
				// stored alternatives
				Iterator<String> newAlternativesIt = newAlternatives.iterator();
				Set<String> deletedTables = new TreeSet<String>();
				while (newAlternativesIt.hasNext()) {
					String altTable = newAlternativesIt.next();
					if (!store.hasTable(altTable)) {
						newAlternativesIt.remove();
						deletedTables.add(altTable);
					}
				}
				// Removing deleted tables from stored alternatives
				if (!deletedTables.isEmpty()) {
					Map<String, Set<String>> removed = new TreeMap<String, Set<String>>();
					removed.put(FEDERATED_META_COLUMN_FAMILY, deletedTables);
					store.storeChanges(null, null, FEDERATED_META_TABLE,
							this.mainTable, null, removed, null);
				}

				// Computing tables we were not aware of
				Set<String> diff = new TreeSet<String>(newAlternatives);
				diff.removeAll(this.alternatives);
				// Actually the following assertion is wrong, as a table might
				// have been deleted...
				// assert newAlternatives.containsAll(this.alternatives);

				// Recording last state
				this.alternatives = newAlternatives;
				return diff;
			} else
				// No alternative found as it was last updated too soon
				return new TreeSet<String>();
		}

		/**
		 * Registering a new alternative for {@link #mainTable}. In case this
		 * alternative was not known, it is stored in the given data store
		 * 
		 * @param table
		 *            the new alternative table ; name should start with
		 *            {@link #mainTable}
		 * @param store
		 *            the store to which register this new alternative ; should
		 *            be the store for a class having {@link #mainTable} as
		 *            original table. null in case no write have ever been done
		 */
		public synchronized void addAlternative(String table, Store store) {
			assert table.startsWith(this.mainTable);
			// Avoid registering original table as an alternative to itself
			if (table.equals(this.mainTable))
				return;
			if (this.alternatives.add(table) && store != null) {
				// We were not aware of that alternative ;
				// let's register in the store
				// Table is FEDERATED_META_TABLE
				// key is the original table
				// family is FEDERATED_META_COLUMN_FAMILY
				// new alternative is the qualifier for an empty cell
				ColumnFamilyData changes = new DefaultColumnFamilyData();
				Map<String, byte[]> change = new TreeMap<String, byte[]>();
				changes.put(FEDERATED_META_COLUMN_FAMILY, change);
				change.put(table, null);
				store.storeChanges(null, null, FEDERATED_META_TABLE,
						this.mainTable, changes, null, null);
			}
		}

		/**
		 * The known alternatives for {@link #mainTable}
		 */
		public Set<String> getAlternatives() {
			return Collections.unmodifiableSet(this.alternatives);
		}
	}

	// Cache for storing table variants (no TTL)
	private static final Map<String /* main table */, TableAlternatives> tablesAlternatives = new TreeMap<String, TableAlternatives>();

	/**
	 * The known alternatives for the given original table. Creates the
	 * alternative in cache.
	 */
	private static TableAlternatives getAlternatives(String mainTable) {
		TableAlternatives alts = tablesAlternatives.get(mainTable);
		if (alts == null) {
			alts = new TableAlternatives(mainTable);
			tablesAlternatives.put(mainTable, alts);
		}
		return alts;
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
	 * @param alternativeTable
	 *            the (possibly new) alternative
	 * @param store
	 *            the store in which storing alternative table ; should be the
	 *            store for a class having {@link #mainTable} as original table
	 * @see TableAlternatives#addAlternative(String, Store)
	 */
	private static void registerTable(String mainTable,
			String alternativeTable, Store store) {
		getAlternatives(mainTable).addAlternative(alternativeTable, store);
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

	public String PersistingElementOverFederatedTable.getTable() {
		if (this.tablePostfix != null)
			return this.getMainTable() + this.tablePostfix;
		else
			return this.getMainTable();
	}

	/**
	 * The original table in which this persisting element would have been
	 * stored in case it would not have been federated. The result can differ
	 * from that one of {@link PersistingElement#getTable()} in case used
	 * alternative table is known and different from this table.
	 */
	public String PersistingElementOverFederatedTable.getMainTable() {
		return super.getTable();
	}

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
					+ this.getMainTable() + this.tablePostfix
					+ " with postfix " + this.tablePostfix
					+ " while another postfix " + oldPostfix
					+ " was registered");
		}
		if (store != null)
			registerTable(this.getMainTable(), this.getMainTable()
					+ this.tablePostfix, store);
	}

	// jut to be sure
	declare error: set(* PersistingElementOverFederatedTable.tablePostfix) && !withincode(private void PersistingElementOverFederatedTable.setTablePostfix(..)) : "Avoid setting this attribute directly ; use setTablePostfix(String postfix, Store store) instead";

	/**
	 * The list of tables where to find this object from what we can guess in
	 * order of probability.
	 */
	private List<String> PersistingElementOverFederatedTable.getKnownPossibleTables() {
		List<String> ret = new LinkedList<String>();
		String mainTable = super.getTable();
		// Table for this object is already known
		if (this.tablePostfix != null) {
			ret.add(mainTable + this.tablePostfix);
			return ret;
		}

		TableAlternatives alternatives = getAlternatives(mainTable);
		Set<String> possibilities = new TreeSet<String>(
				alternatives.getAlternatives());
		// First, asks postfix if the element already knows
		String possiblePostfixedTable = mainTable + this.getTablePostfix();
		ret.add(possiblePostfixedTable);
		possibilities.remove(possiblePostfixedTable);

		// Otherwise, let's see main table
		ret.add(mainTable);
		// And then all other known tables
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
	private Collection<String> PersistingElementOverFederatedTable.getPossibleTablesWithAnUpdate(
			Store store) {
		assert this.tablePostfix == null;
		return getAlternatives(this.getMainTable()).updateAlternatives(store);
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
		FederatedMode fm = this.getClass().getAnnotation(Persisting.class)
				.federated();
		if (this.tablePostfix != null && fm.isCheckForChangingPostfix()) {
			// Let's check whether a new computation for table postfix changes
			// its value...
			String computedPostfix = this.getTablePostfix();
			if (!this.tablePostfix.equals(computedPostfix)) {
				// Could still be forgiven in legacy mode
				if (this.tablePostfix.length() != 0)
					throw new IllegalStateException(this
							+ " already registered in table "
							+ this.getMainTable() + " with postfix "
							+ this.tablePostfix
							+ " while computed postfix states now "
							+ computedPostfix);
			}
		}
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
			ReadWrite mode) {
		if (this.tablePostfix != null) {
			this.checkTablePostfixHasNotChanged();
			return false;
		}

		Consistency consistencyLevel = this.getClass()
				.getAnnotation(Persisting.class).federated()
				.getConsistency(mode);

		Store store = this.getStore();

		if (consistencyLevel.compareTo(Consistency.CONSISTENT_WITH_LEGACY) >= 0) {
			String mainTable = this.getMainTable();
			String id = this.getIdentifier();
			Set<String> testedTables = new HashSet<String>();

			// First trying with expected table
			if (this.testTableLocation(mainTable + this.getTablePostfix(), id,
					mainTable, testedTables, store))
				return true;

			// Then trying with legacy table
			if (this.testTableLocation(mainTable, id, mainTable, testedTables,
					store))
				return true;

			// In case of heavy consistency mode, test all possible tables
			if (consistencyLevel.compareTo(Consistency.CONSISTENT) >= 0) {
				for (String t : this.getKnownPossibleTables()) {
					if (this.testTableLocation(t, id, mainTable, testedTables,
							store))
						return true;
				}
				// No found yet ; querying possible alternatives from store
				for (String t : this.getPossibleTablesWithAnUpdate(store)) {
					if (this.testTableLocation(t, id, mainTable, testedTables,
							store))
						return true;
				}
			}
		}

		// Still not found ; setting postfix to computed value
		// Only registering in case we are sure table exists (or about to)
		this.setTablePostfix(this.getTablePostfix(), mode.isRead() ? null
				: store);
		return false;
	}

	private boolean PersistingElementOverFederatedTable.testTableLocation(
			String t, String id, String mainTable, Set<String> alreadyTested,
			Store store) {
		assert t.startsWith(mainTable);
		if (alreadyTested.add(t)) {
			if (store.exists(this, t, id)) {
				this.setTablePostfix(t.substring(mainTable.length()), store);
				return true;
			}
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

	// ===================================
	// element-level operations
	// ===================================

	// Store
	void around(PersistingElementOverFederatedTable self, String table,
			Store store):
		call(void Store+.storeChanges(..))
		&& within(com.googlecode.n_orm..*) && !within(*Test) && !within(FederatedTableManagement)
		&& target(store)
		&& args(self, Map<String, Field>, table, ..) {

		self.findTableLocation(ReadWrite.WRITE);

		// Postfixing table name if necessary
		if (!table.endsWith(self.tablePostfix)) {
			String actualTable = table + self.tablePostfix;
			// Should register table (even if looks like done when invoking
			// findTableLocation) as we may be adding a postfix for a super
			// table (remember federated mode is inherited and immutable)
			registerTable(table, actualTable, store);
			table = actualTable;
		}

		proceed(self, table, store);
	}

	// Activate
	ColumnFamilyData around(PersistingElementOverFederatedTable self,
			String table):
		call(ColumnFamilyData Store.get(PersistingElement,String,String,Map<String, Field>))
		&& within(com.googlecode.n_orm..*) && !within(*Test) && !within(FederatedTableManagement)
		&& args(self, table, ..)
		// No need to around when postfix is already known as getTable returns the actual table
		&& if(self.tablePostfix == null) {
		if (self.findTableLocation(ReadWrite.READ)) // Element exists
			return proceed(self, table + self.tablePostfix);
		else
			// Element does not exists
			return null;
	}

	// Exists
	boolean around(PersistingElementOverFederatedTable self):
		call(boolean Store.exists(PersistingElement, String, String))
		&& within(com.googlecode.n_orm..*) && !within(*Test) && !within(FederatedTableManagement)
		&& args(self,..)
		// No need to around when postfix is already known as getTable returns the actual table
		&& if(self.tablePostfix == null) {
		return self.findTableLocation(ReadWrite.READ);
	}

	// Delete
	void around(PersistingElementOverFederatedTable self, String table):
		call(void Store+.delete(..))
		&& within(com.googlecode.n_orm..*) && !within(*Test) && !within(FederatedTableManagement)
		&& args(self, table, ..) {
		self.findTableLocation(ReadWrite.READ_OR_WRITE);
		if (!table.endsWith(self.tablePostfix))
			table = table + self.tablePostfix;
		proceed(self, table);
		self.setTablePostfix(null, null);
	}

	// ===================================
	// family-level operations
	// ===================================

	// Exists
	boolean around(PersistingElementOverFederatedTable self, String table):
		call(boolean Store.exists(PersistingElement, Field, String, String, String))
		&& within(com.googlecode.n_orm..*) && !within(*Test) && !within(FederatedTableManagement)
		&& args(self,Field, table, ..)
		// No need to around when postfix is already known as getTable returns the actual table
		&& if(self.tablePostfix == null) {

		// Element does not exists
		if (!self.findTableLocation(ReadWrite.READ))
			return false;

		return proceed(self, self.getTable());
	}

	// Get column
	byte[] around(PersistingElementOverFederatedTable self, String table):
		call(byte[] Store.get(PersistingElement, Field, String, String, String, String))
		&& within(com.googlecode.n_orm..*) && !within(*Test) && !within(FederatedTableManagement)
		&& args(self,Field, table, ..)
		// No need to around when postfix is already known as getTable returns the actual table
		&& if(self.tablePostfix == null) {

		// Element does not exists
		if (!self.findTableLocation(ReadWrite.READ))
			return null;

		return proceed(self, self.getTable());
	}

	// Get all columns
	Map<String, byte[]> around(PersistingElementOverFederatedTable self,
			String table):
		(		call(Map<String, byte[]> Store.get(PersistingElement, Field, String, String, String))
			||	call(Map<String, byte[]> Store.get(PersistingElement, Field, String, String, String, Constraint))
		)
		&& within(com.googlecode.n_orm..*) && !within(*Test) && !within(FederatedTableManagement)
		&& args(self,Field, table, ..)
		// No need to around when postfix is already known as getTable returns the actual table
		&& if(self.tablePostfix == null) {

		// Element does not exists
		if (!self.findTableLocation(ReadWrite.READ))
			return null;

		return proceed(self, self.getTable());
	}

	// ===================================
	// global-level operations
	// ===================================

	private static abstract class GlobalAction<T> {

		/**
		 * Runs the query on one possible alternative table
		 */
		protected abstract T localRun(String table);

		/**
		 * Merging two results for different alternative table into a single
		 * one. Order in which results are merged is unknown.
		 * 
		 * @param lhs
		 *            either result for the first table or the previously
		 *            aggregated result.
		 */
		protected abstract T add(T lhs, T rhs);

		private Callable<T> createLocalAction(final String table) {
			return new Callable<T>() {

				@Override
				public T call() throws Exception {
					return localRun(table);
				}

			};
		}

		/**
		 * Runs {@link #localRun(String) the action} on all referenced
		 * alternative tables (including main table) and
		 * {@link #add(Object, Object) aggregates} results.
		 * @param c 
		 */
		public T globalRun(String table, Store store, Constraint c) {

			// Table was set in the query
			if (c != null && (c instanceof ConstraintWithTable)) {
				return this.localRun(((ConstraintWithTable)c).getTable());
			}

			ExecutorService exec = Executors
					.newFixedThreadPool(ParallelGlobalSearch);
			Collection<Future<T>> results = new LinkedList<Future<T>>();

			// looking count in the main table
			results.add(exec.submit(this.createLocalAction(table)));

			TableAlternatives alts = getAlternatives(table);
			// Making sure we are aware of all possible alternative tables
			alts.updateAlternatives(store);

			for (final String t : alts.getAlternatives()) {
				assert !t.equals(table);
				results.add(exec.submit(this.createLocalAction(t)));
			}

			// Waiting for results to show up
			exec.shutdown();
			try {
				exec.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				throw new DatabaseNotReachedException(e);
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

	// Count
	long around(final Class<? extends PersistingElement> clazz,
			final String table, final Constraint c, final Store store):
		call(long Store.count(Class<? extends PersistingElement>, String, Constraint))
		&& within(com.googlecode.n_orm..*) && !within(*Test) && !within(FederatedTableManagement)
		&& target(store)
		&& args(clazz, table, c) {
		if (!clazz.getAnnotation(Persisting.class).federated().isFederated()) {
			return proceed(clazz, table, c, store);
		}

		return new GlobalAction<Long>() {

			@Override
			protected Long localRun(final String t) {
				return store.count(clazz, t, c);
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
		private final String table;
		private final Row row;

		public RowWithTable(String table, Row row) {
			super();
			this.table = table;
			this.row = row;
		}

		public String getTable() {
			return table;
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
		private final String table;
		private final CloseableKeyIterator iterator;

		public CloseableKeyIteratorWithTable(String table,
				CloseableKeyIterator iterator) {
			super();
			this.table = table;
			this.iterator = iterator;
		}

		public String getTable() {
			return table;
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
			return r == null ? null : new RowWithTable(this.getTable(), r);
		}

		@Override
		public void remove() {
			iterator.remove();
		}

	}

	/**
	 * An {@link CloseableIterator} composite able to iterate over a set of
	 * {@link CloseableIterator}s. Elements are selected from composed iterators
	 * so that they are iterated
	 * {@link PersistingElement#compareTo(PersistingElement) in an ordered way}.
	 */
	private static class AggregatingIterator implements CloseableKeyIterator {

		/**
		 * A pointer on a composed iterator..
		 */
		private static class IteratorStatus {
			/**
			 * The result of an iterated element. The iterated element is
			 * considered as iterated over only when
			 * {@link ResultReadyToGo#getResult()} is called.
			 */
			private class ResultReadyToGo {

				/**
				 * Get the iterated row and marks it as iterated over, that is
				 * iterator pointed by {@link IteratorStatus} will be called
				 * {@link Iterator#next()} in order to know what is the next
				 * element to be iterated, instead of returning this element
				 * again.
				 * 
				 * @return the iterated row
				 */
				public Row getResult() {
					Row ret = IteratorStatus.this.next;
					IteratorStatus.this.next = null;
					return ret;
				}

				/**
				 * The key of the result. This methods leave the element as the
				 * next element to be iterated.
				 * 
				 * @return
				 */
				private String getKey() {
					return IteratorStatus.this.next.getKey();
				}
			}

			/**
			 * The pointed iterator
			 */
			private final CloseableKeyIterator it;
			/**
			 * The next row to be iterated
			 */
			private Row next = null;
			/**
			 * Whether pointed iterator is empty and closed
			 */
			private boolean done = false;

			public IteratorStatus(CloseableKeyIterator it) {
				this.it = it;
			}

			/**
			 * Grabs the next element to be iterated if no known yet.
			 */
			private void prepareNext() {
				try {
					if (this.done)
						return;
					if (this.next == null) {
						// First call or last result was iterated using
						// ResultReadyToGo.getResult()
						if (this.it.hasNext()) {
							this.next = this.it.next();
						} else {
							// No more elements in this iterator ; closing
							this.close();
						}
					}
				} finally {
					assert this.done == (this.next == null);
				}
			}

			/**
			 * @see CloseableKeyIterator#hasNext()
			 */
			public boolean hasNext() {
				this.prepareNext();
				return this.next != null;
			}

			/**
			 * Returns the next element to be iterated if this element is lower
			 * than parameter. Lower means with a lower key as can be found by
			 * {@link ResultReadyToGo#getKey()}.
			 * 
			 * @param r
			 *            null or a previously iterated key
			 * @return r if it is null or lower than the next element of this
			 *         iterator or the row to be removed from this iterator in
			 *         case {@link ResultReadyToGo#getResult()} is called
			 * @see CloseableKeyIterator#next()
			 */
			public ResultReadyToGo getNextIfLower(ResultReadyToGo r) {
				this.prepareNext();

				if (this.done)
					return r;

				assert this.next != null;

				if (r == null || this.next.getKey().compareTo(r.getKey()) < 0) {
					return new ResultReadyToGo();
				} else {
					return r;
				}
			}

			/**
			 * @see CloseableIterator#close()
			 */
			public void close() {
				if (!this.done) {
					this.it.close();
					this.done = true;
				}
			}
		}

		/**
		 * The composed iterators.
		 */
		private Set<IteratorStatus> status = new LinkedHashSet<IteratorStatus>();
		/**
		 * Whether iteration has started.
		 */
		private boolean started = false;

		/**
		 * Adds an iterator to the list of iterators to be explored.
		 * 
		 * @throws IllegalStateException
		 *             in case the iteration has started using
		 *             {@link #hasNext()} or {@link #next()}.
		 */
		public void addIterator(CloseableKeyIterator it) {
			if (this.started)
				throw new IllegalStateException(
						"Cannot add a new iterator when iteration has started");
			this.status.add(new IteratorStatus(it));
		}

		@Override
		public void close() {
			for (IteratorStatus is : this.status) {
				is.close();
			}
		}

		@Override
		public boolean hasNext() {
			this.started = true;
			for (IteratorStatus is : this.status) {
				if (is.hasNext())
					return true;
			}
			return false;
		}

		@Override
		public Row next() {
			this.started = true;
			com.googlecode.n_orm.FederatedTableManagement.AggregatingIterator.IteratorStatus.ResultReadyToGo ret = null;
			for (IteratorStatus is : this.status) {
				ret = is.getNextIfLower(ret);
			}
			return ret.getResult();
		}

		/**
		 * @throws UnsupportedOperationException
		 *             in any case
		 */
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	// Search
	CloseableKeyIterator around(final Class<? extends PersistingElement> clazz,
			final String table, final Constraint c, final int limit,
			final Map<String, Field> families, final Store store):
		call(CloseableKeyIterator Store.get(Class<? extends PersistingElement>, String, Constraint,int, Map<String, Field>))
		&& within(com.googlecode.n_orm..*) && !within(*Test) && !within(FederatedTableManagement)
		&& target(store)
		&& args(clazz, table, c, limit, families) {
		if (!clazz.getAnnotation(Persisting.class).federated().isFederated()) {
			return proceed(clazz, table, c, limit, families, store);
		}

		return new GlobalAction<CloseableKeyIterator>() {

			@Override
			protected CloseableKeyIterator localRun(final String t) {
				return new CloseableKeyIteratorWithTable(t, store.get(clazz, t,
						c, limit, families));
			}

			@Override
			protected CloseableKeyIterator add(CloseableKeyIterator lhs,
					CloseableKeyIterator rhs) {
				if (lhs == null)
					return rhs;
				if (!(lhs instanceof AggregatingIterator)) {
					AggregatingIterator ret = new AggregatingIterator();
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
			String mainTable = self.getMainTable();
			String actualTable = row.getTable();
			assert actualTable.startsWith(mainTable);
			self.setTablePostfix(actualTable.substring(mainTable.length()),
					self.getStore());
		}
	}

	// Search with query
	// Add a new method to search elements of a given table
	private String SearchableClassConstraintBuilder.table = null;

	public String SearchableClassConstraintBuilder<T extends PersistingElementOverFederatedTable>.getTable() {
		return this.table;
	}

	public SearchableClassConstraintBuilder<T> SearchableClassConstraintBuilder<T extends PersistingElementOverFederatedTable>.inTable(
			String table) {
		if (this.table != null && !this.table.equals(table))
			throw new IllegalArgumentException(
					"This query is already limited to table " + this.table
							+ " ; cannot set it to " + table);

		if (!PersistingElementOverFederatedTable.class.isAssignableFrom(this
				.getClazz()))
			throw new IllegalArgumentException("Class "
					+ this.getClazz().getName()
					+ " is not federated ; cannot assign table in query");

		String mainTable = PersistingMixin.getInstance().getTable(
				this.getClazz());
		if (!table.startsWith(mainTable))
			throw new IllegalArgumentException("Table " + table
					+ " is not an alternative table for class "
					+ this.getClazz().getName() + " as it should start with "
					+ mainTable);

		this.table = table;
		return this;
	}

	// We're using Constraint to transmit searched table
	public static class ConstraintWithTable extends Constraint {
		private final String table;
		private final Constraint constraint;

		public ConstraintWithTable(Constraint c, String table) {
			super(c == null ? null : c.getStartKey(), c == null ? null : c
					.getEndKey());
			this.constraint = c;
			this.table = table;
		}

		public String getTable() {
			return this.table;
		}

		public Constraint getConstraint() {
			return this.constraint;
		}
	}

	// Created a tabled constraint in case query was set a table
	Constraint around(SearchableClassConstraintBuilder self): 
		execution(Constraint ClassConstraintBuilder.getConstraint())
		&& target(self) 
		&& if(self.getTable() != null) {
		return new ConstraintWithTable(proceed(self), self.getTable());
	}
}
