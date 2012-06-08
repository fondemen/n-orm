package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.googlecode.n_orm.storeapi.Store;

public aspect FederatedTableManagement {

	public static final String FEDERATED_META_TABLE = "n-orm-federated-tables";
	public static final String FEDERATED_META_COLUMN_FAMILY = "t";

	/**
	 * The time (in s) during which table alternatives are not loaded again from
	 * the base ; default is 10min
	 */
	public static long TableAlternativeCacheTTLInS = 600;

	// REM: a federated element can only inherit federated elements
	declare parents: (@Persisting(federated=true) *) implements PersistingElement, PersistingElementOverFederatedTable;

	private final static class TableAlternatives {
		private final String mainTable;
		private long lastUpdate = -TableAlternativeCacheTTLInS;
		private Set<String> alternatives = new TreeSet<String>();

		public TableAlternatives(String mainTable) {
			this.mainTable = mainTable;
		}

		/**
		 * Updates alternatives according to meta-informations stored in the
		 * store
		 * 
		 * @param store
		 *            where alternative meta-information should be retrieved
		 *            from
		 * @return tables that appeared with the update
		 */
		protected Set<String> updateAlternatives(Store store) {

			if ((this.lastUpdate + TableAlternativeCacheTTLInS) > System
					.currentTimeMillis()) {
				Map<String, byte[]> res = store.get(null, null,
						FEDERATED_META_TABLE, this.mainTable,
						FEDERATED_META_COLUMN_FAMILY);
				Set<String> newAlternatives = res == null ? new TreeSet<String>()
						: new TreeSet<String>(res.keySet());
				Set<String> diff = new TreeSet<String>(newAlternatives);
				diff.removeAll(this.alternatives);
				assert newAlternatives.containsAll(this.alternatives);
				this.alternatives = newAlternatives;
				return diff;
			} else
				return new TreeSet<String>();
		}

		public void addAlternative(String table, Store store) {
			assert table.startsWith(this.mainTable);
			if (table.equals(this.mainTable))
				return;
			if (this.alternatives.add(table)) {
				Map<String, Map<String, byte[]>> changes = new TreeMap<String, Map<String, byte[]>>();
				Map<String, byte[]> change = new TreeMap<String, byte[]>();
				changes.put(FEDERATED_META_COLUMN_FAMILY, change);
				change.put(table, null);
				store.storeChanges(null, null, FEDERATED_META_TABLE,
						this.mainTable, changes, null, null);
			}
		}

		public Set<String> getAlternatives() {
			return this.alternatives;
		}
	}

	// Cache for storing table variants
	private static final Map<String /* main table */, TableAlternatives> tablesAlternatives = new TreeMap<String, TableAlternatives>();

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

	private static void registerTable(String mainTable,
			String alternativeTable, Store store) {
		getAlternatives(mainTable).addAlternative(alternativeTable, store);
	}

	// The postfix for tables ; null if table is not known
	private transient String PersistingElementOverFederatedTable.tablePostfix = null;

	public String PersistingElementOverFederatedTable.getTable() {
		if (this.tablePostfix != null) {
			return this.getMainTable() + this.tablePostfix;
		} else
			return this.getMainTable();
	}

	public String PersistingElementOverFederatedTable.getMainTable() {
		return super.getTable();
	}

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
	 * The list of tables where to find this object except those that can be
	 * guessed by
	 * {@link PersistingElementOverFederatedTable#getKnownPossibleTables()}
	 */
	private List<String> PersistingElementOverFederatedTable.getPossibleTablesWithAnUpdate(
			Collection<String> alreadyTestedTables, Store store) {
		assert this.tablePostfix == null;

		List<String> ret = new LinkedList<String>();
		String mainTable = super.getTable();
		TableAlternatives alternatives = getAlternatives(mainTable);
		ret.addAll(alternatives.updateAlternatives(store));
		ret.removeAll(alreadyTestedTables);

		return ret;
	}

	// Store
	void around(PersistingElementOverFederatedTable self, String table,
			Store store):
		call(void Store+.storeChanges(..))
		&& within(StorageManagement)
		&& target(store)
		&& args(self, Map<String, Field>, table, ..) {
		// We now have to definitely choose the proper table
		if (self.tablePostfix == null)
			self.tablePostfix = self.getTablePostfix();

		// Postfixing table name if necessary + registering alternative in the
		// cache
		if (!table.endsWith(self.tablePostfix)) {
			String actualTable = table + self.tablePostfix;
			registerTable(table, actualTable, store);
			table = actualTable;
		}

		proceed(self, table, store);
	}
	
	private static abstract class Action<T> {
		abstract T performAction(PersistingElementOverFederatedTable self, String table);
		
		abstract boolean isAnswerValid(T ans);
		
		public T run(PersistingElementOverFederatedTable self, Store store) {
			String mainTable = self.getMainTable();
			// ExecutorService pe = Executors.newCachedThreadPool();
			// First trying with known possible tables
			List<String> knownPossibleTables = self.getKnownPossibleTables();
			for (String t : knownPossibleTables) {
				assert t.startsWith(mainTable);
				T ret = this.performAction(self, t);
				if (this.isAnswerValid(ret)) {
					self.tablePostfix = t.substring(mainTable.length());
					return ret;
				}
			}
			// Then retrying with tables from store
			for (String t : self.getPossibleTablesWithAnUpdate(
					knownPossibleTables, store)) {
				assert t.startsWith(mainTable);
				T ret = this.performAction(self, t);
				if (this.isAnswerValid(ret)) {
					self.tablePostfix = t.substring(mainTable.length());
					return ret;
				}
			}
			return null;
		}
	}

	// Activate
	Map<String, Map<String, byte[]>> around(
			PersistingElementOverFederatedTable self, final String table, final String id,
			final Map<String, Field> families, final Store store):
		call(Map<String, Map<String, byte[]>> Store.get(PersistingElement,String,String,Map<String, Field>))
		&& within(StorageManagement)
		&& this(self)
		&& target(store)
		&& args(PersistingElement, table, id, families) {
		return new Action<Map<String, Map<String, byte[]>>>() {
			Map<String, Map<String, byte[]>> performAction(PersistingElementOverFederatedTable self, String table) {
				return store.get(self, table, id, families);
			}
			boolean isAnswerValid(Map<String, Map<String, byte[]>> ans) {
				return ans != null && !ans.isEmpty();
			}
		}.run(self, store);
	}
}
