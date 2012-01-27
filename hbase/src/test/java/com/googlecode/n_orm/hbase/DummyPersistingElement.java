package com.googlecode.n_orm.hbase;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PersistingElementListener;
import com.googlecode.n_orm.UnknownColumnFamily;
import com.googlecode.n_orm.PropertyManagement.PropertyFamily;
import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.storeapi.Store;

public class DummyPersistingElement implements PersistingElement {

	@Override
	public void activate(Object[] arg0) throws DatabaseNotReachedException {
		// TODO Auto-generated method stub

	}

	@Override
	public String getTable() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Field> getKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnFamily<?> getColumnFamily(String columnFamilyName)
			throws UnknownColumnFamily {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnFamily<?> getColumnFamily(Object collection)
			throws UnknownColumnFamily {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void activate(String[] families) throws DatabaseNotReachedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void activateColumnFamily(String name)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void activateColumnFamily(String name, Object from, Object to)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void activateColumnFamilyIfNotAlready(String name)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void activateColumnFamilyIfNotAlready(String name,
			long lastActivationTimeoutMs) throws DatabaseNotReachedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void activateColumnFamilyIfNotAlready(String name, Object from,
			Object to) throws DatabaseNotReachedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void activateColumnFamilyIfNotAlready(String name,
			long lastActivationTimeoutMs, Object from, Object to)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void activateFromRawData(Set<String> arg0,
			Map<String, Map<String, byte[]>> arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public Collection<ColumnFamily<?>> getColumnFamilies() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void delete() throws DatabaseNotReachedException {
		// TODO Auto-generated method stub

	}

	@Override
	public PersistingElement getCachedVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean exists() throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean existsInStore() throws DatabaseNotReachedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void activateIfNotAlready(String[] families)
			throws DatabaseNotReachedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void activateIfNotAlready(long lastActivationTimeoutMs,
			String[] families) throws DatabaseNotReachedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void addPersistingElementListener(PersistingElementListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void checkIsValid() throws IllegalStateException {
		// TODO Auto-generated method stub

	}

	@Override
	public int compareTo(PersistingElement rhs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Set<String> getColumnFamilyNames() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getIdentifier() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getFullIdentifier() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<Class<? extends PersistingElement>> getPersistingSuperClasses() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Store getStore() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PropertyFamily getPropertiesColumnFamily() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasChanged() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean hasListener(PersistingElementListener arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isKnownAsExistingInStore() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isKnownAsNotExistingInStore() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void removePersistingElementListener(
			PersistingElementListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setPOJO(boolean arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setStore(Store store) {
		// TODO Auto-generated method stub

	}

	@Override
	public void store() throws DatabaseNotReachedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateFromPOJO() {
		// TODO Auto-generated method stub

	}

}
