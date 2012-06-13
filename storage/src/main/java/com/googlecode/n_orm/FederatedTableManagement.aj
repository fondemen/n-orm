package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
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
	declare parents: (@Persisting(federated!=Persisting.FederatedMode.NONE) *) implements PersistingElement, PersistingElementOverFederatedTable;

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
			long now = System.currentTimeMillis();
			if ((this.lastUpdate + TableAlternativeCacheTTLInS) < now) {
				this.lastUpdate = now;
				Map<String, byte[]> res = store.get(null, null,
						FEDERATED_META_TABLE, this.mainTable,
						FEDERATED_META_COLUMN_FAMILY);
				Set<String> newAlternatives = res == null ? new TreeSet<String>()
						: new TreeSet<String>(res.keySet());
				
				//Checking for deleted tables
				Iterator<String> newAlternativesIt = newAlternatives.iterator();
				Set<String> deletedTables = new TreeSet<String>();
				while (newAlternativesIt.hasNext()) {
					String altTable = newAlternativesIt.next();
					if (!store.hasTable(altTable)) {
						newAlternativesIt.remove();
						deletedTables.add(altTable);
					}
				}
				if (!deletedTables.isEmpty()) {
					Map<String, Set<String>> removed = new TreeMap<String, Set<String>>();
					removed.put(FEDERATED_META_COLUMN_FAMILY, deletedTables);
					store.storeChanges(null, null, FEDERATED_META_TABLE, this.mainTable, null, removed , null);
				}
				
				Set<String> diff = new TreeSet<String>(newAlternatives);
				diff.removeAll(this.alternatives);
				//Actually the following assertion is wrong, as a table might have been deleted...
				//assert newAlternatives.containsAll(this.alternatives);
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
	private transient String PersistingElementOverFederatedTable.tablePostfix;

	public String PersistingElementOverFederatedTable.getTable() {
		if (this.tablePostfix != null) {
			return this.getMainTable() + this.tablePostfix;
		} else
			return this.getMainTable();
	}

	public String PersistingElementOverFederatedTable.getMainTable() {
		return super.getTable();
	}
	
	private void PersistingElementOverFederatedTable.setTablePostfix(String postfix, Store store) {
		String oldPostfix = this.tablePostfix;
		this.tablePostfix = postfix;
		this.checkTablePostfixHasNotChanged();
		switch (this.getClass().getAnnotation(Persisting.class).federated()) {
		case FAST_CHECKED:
		case CONSISTENT:
			if (oldPostfix != null && !oldPostfix.equals(this.tablePostfix)) {
				throw new IllegalStateException("Found " + this
						+ " from table " + this.getMainTable() + " with postfix "
						+ this.tablePostfix + " while another postfix "
						+ oldPostfix + " was registered");
			}
		}
		registerTable(this.getMainTable(), this.getMainTable()+this.tablePostfix, store);
	}
	
	//jut to be sure
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
	
	private void PersistingElementOverFederatedTable.checkTablePostfixHasNotChanged() {
		if (this.getClass().getAnnotation(Persisting.class).federated()
				.equals(Persisting.FederatedMode.FAST_CHECKED)) {
			//Let's check whether a new computation for table postfix changes its value...
			String computedPostfix = this.getTablePostfix();
			if (!this.tablePostfix.equals(computedPostfix)) {
				throw new IllegalStateException(this
						+ " already registered in table "
						+ this.getMainTable() + " with postfix "
						+ this.tablePostfix
						+ " while computed postfix states now "
						+ computedPostfix);
			}
		}
	}

	// Store
	void around(PersistingElementOverFederatedTable self, String table,
			String id, Store store):
		call(void Store+.storeChanges(..))
		&& within(StorageManagement)
		&& target(store)
		&& args(self, Map<String, Field>, table, id, ..) {

		// consistent mode ; let's see where this object can be found already
		if (self.tablePostfix == null
				&& Persisting.FederatedMode.CONSISTENT.equals(self.getClass()
						.getAnnotation(Persisting.class).federated())) {
			// Should setup self.tablePostfix if it exists
			boolean exists = new RowExistsAction(store, id).run(self, store);
			assert exists == (self.tablePostfix != null);
		}

		// We now have to definitely choose the proper table
		if (self.tablePostfix == null) {
			self.setTablePostfix(self.getTablePostfix(), store);
		} else {
			self.checkTablePostfixHasNotChanged();
		}

		// Postfixing table name if necessary + registering alternative in the
		// cache
		if (!table.endsWith(self.tablePostfix)) {
			String actualTable = table + self.tablePostfix;
			registerTable(table, actualTable, store);
			table = actualTable;
		}

		proceed(self, table, id, store);
	}

	private static abstract class Action<T> {
		abstract T performAction(PersistingElementOverFederatedTable self,
				String table);

		abstract boolean isAnswerValid(T ans);

		public T run(PersistingElementOverFederatedTable self, Store store) {
			String mainTable = self.getMainTable();
			// ExecutorService pe = Executors.newCachedThreadPool();
			// First trying with known possible tables
			List<String> knownPossibleTables = self.getKnownPossibleTables();
			T ret = null;
			for (String t : knownPossibleTables) {
				assert t.startsWith(mainTable);
				ret = this.performAction(self, t);
				if (this.isAnswerValid(ret)) {
					self.setTablePostfix(t.substring(mainTable.length()), store);
					return ret;
				}
			}
			// Then retrying with tables from store
			for (String t : self.getPossibleTablesWithAnUpdate(
					knownPossibleTables, store)) {
				assert t.startsWith(mainTable);
				ret = this.performAction(self, t);
				if (this.isAnswerValid(ret)) {
					self.setTablePostfix(t.substring(mainTable.length()), store);
					return ret;
				}
			}
			return ret;
		}
	}

	// Activate
	Map<String, Map<String, byte[]>> around(
			PersistingElementOverFederatedTable self, final String table,
			final String id, final Map<String, Field> families,
			final Store store):
		call(Map<String, Map<String, byte[]>> Store.get(PersistingElement,String,String,Map<String, Field>))
		&& within(StorageManagement)
		&& target(store)
		&& args(self, table, id, families) {
		return new Action<Map<String, Map<String, byte[]>>>() {
			Map<String, Map<String, byte[]>> performAction(
					PersistingElementOverFederatedTable self, String table) {
				return store.get(self, table, id, families);
			}

			boolean isAnswerValid(Map<String, Map<String, byte[]>> ans) {
				return ans != null && !ans.isEmpty();
			}
		}.run(self, store);
	}

	private static class RowExistsAction extends Action<Boolean> {
		private Store store;
		private String id;

		public RowExistsAction(Store store, String id) {
			this.store = store;
			this.id = id;
		}

		Boolean performAction(PersistingElementOverFederatedTable self,
				String table) {
			return store.exists(self, table, id);
		}

		boolean isAnswerValid(Boolean ans) {
			return ans.booleanValue();
		}
	}

	// Exists
	boolean around(PersistingElementOverFederatedTable self,
			final String table, final String id, final Store store):
		call(boolean Store.exists(PersistingElement, String, String))
		&& within(StorageManagement)
		&& target(store)
		&& args(self, table, id) {
		Boolean ret = new RowExistsAction(store, id).run(self, store);
		return ret != null && ret.booleanValue();
	}
}
