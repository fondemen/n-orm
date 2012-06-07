package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.googlecode.n_orm.storeapi.Store;

public aspect FederatedTableManagement {

	public static final String FEDERATED_META_TABLE = "n-orm-federated-tables";
	public static final String FEDERATED_META_COLUMN_FAMILY = "t";

	declare parents: (@Persisting(federated=true) *) implements PersistingElement, PersistingElementOverFederatedTable;

	private String PersistingElementOverFederatedTable.table = null;
	
	public String PersistingElementOverFederatedTable.getTable() {
		return this.table == null ? super.getTable() : this.table;
	}
	
	private Set<String> PersistingElementOverFederatedTable.getPossibleTables() {
		Map<String, byte[]> res = this.getStore().get(null, null, FEDERATED_META_TABLE, super.getTable(), FEDERATED_META_COLUMN_FAMILY);
		Set<String> ret = res == null ? new TreeSet<String>() : new TreeSet<String>(res.keySet());
		ret.add(super.getTable());
		return ret;
	}
	
	void around(PersistingElementOverFederatedTable self, String table, Store store):
		call(void Store.storeChanges(..))
		&& within(StorageManagement)
		&& this(self)
		&& target(store)
		&& args(PersistingElement, Map<String, Field>, table, ..) {
		String clazzTable = PersistingMixin.getInstance().getTable(self.getClass());
		if (table.equals(clazzTable)) {
			if (self.table == null) {
				self.table = clazzTable + self.getTablePostfix();
				//Registering the alternative table to the list of possible tables to search from
				if (! table.equals(self.table)) {
					Map<String, Map<String, byte[]>> changes = new TreeMap<String, Map<String, byte[]>>();
					Map<String, byte[]> change = new TreeMap<String, byte[]>();
					changes.put(FEDERATED_META_COLUMN_FAMILY, change);
					change.put(self.table, null);
					store.storeChanges(null, null, FEDERATED_META_TABLE, clazzTable, changes, null, null);
				}
			}
			proceed(self, self.table, store);
			
		} else
			proceed(self, table, store);
	}
	
	Map<String, Map<String, byte[]>> around(PersistingElementOverFederatedTable self, String table, Map<String, Field> families, Store store):
		call(Map<String, Map<String, byte[]>> Store.get(PersistingElement,String,String,Map<String, Field>))
		&& within(StorageManagement)
		&& this(self)
		&& target(store)
		&& args(PersistingElement, String, table, families) {
		if (self.table != null)
			return proceed(self, self.table, families, store);
		else {
			
			//ExecutorService pe = Executors.newCachedThreadPool();
			for (String t : self.getPossibleTables()) {
				Map<String, Map<String, byte[]>> ret = store.get(self, t, self.getIdentifier(), families);
				if (ret != null && !ret.isEmpty()) {
					self.table = t;
					return ret;
				}
			}
			return null;
		}
	}
}
