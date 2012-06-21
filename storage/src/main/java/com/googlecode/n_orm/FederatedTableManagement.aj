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

import com.googlecode.n_orm.FederatedMode.Consistency;
import com.googlecode.n_orm.FederatedMode.ReadWrite;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.Store;

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
	 * The time (in s) during which table alternatives are not loaded again from
	 * the base ; default is 10min
	 */
	public static long TableAlternativeCacheTTLInS = 600;

	// REM: a federated element can only inherit federated elements
	declare parents: (@Persisting(federated!=FederatedMode.NONE) *) implements PersistingElement, PersistingElementOverFederatedTable;

	/**
	 * A place where to register alternatives for an original tables.
	 * Alternative tables can be registered or updated from the store using
	 * table {@link FederatedTableManagement#FEDERATED_META_TABLE} and family
	 * {@link FederatedTableManagement#FEDERATED_META_COLUMN_FAMILY}. Updates
	 * occurs at most each
	 * {@link FederatedTableManagement#TableAlternativeCacheTTLInS}
	 * milliseconds.
	 */
	private final static class TableAlternatives {
		/**
		 * The original table
		 */
		private final String mainTable;

		/**
		 * When alternatives for {@link #mainTable} was last updated
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
		 *            original table
		 */
		public synchronized void addAlternative(String table, Store store) {
			assert table.startsWith(this.mainTable);
			// Avoid registering original table as an alternative to itself
			if (table.equals(this.mainTable))
				return;
			if (this.alternatives.add(table)) {
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
		registerTable(this.getMainTable(), this.getMainTable()
				+ this.tablePostfix, store);
	}

	// jut to be sure
	declare error: set(* PersistingElementOverFederatedTable.tablePostfix) && !withincode(private void PersistingElementOverFederatedTable.setTablePostfix(String, Store)) : "Avoid setting this attribute directly ; use setTablePostfix(String postfix, Store store) instead";

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

		// No consistency check
		if (consistencyLevel.compareTo(Consistency.NONE) <= 0) {
			this.setTablePostfix(this.getTablePostfix(), store);
			return false;
		}

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

		// Still not found ; setting postfix to computed value
		this.setTablePostfix(this.getTablePostfix(), store);
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
		self.setTablePostfix(null, self.getStore());
	}

	// ===================================
	// family-level operations
	// ===================================
}
