package com.googlecode.n_orm.query;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.TreeSet;

import com.googlecode.n_orm.CloseableIterator;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.storeapi.Constraint;


public class ClassConstraintBuilder<T extends PersistingElement> {

	private Class<T> clazz;
	private Map<Field, Object> keyValues = new HashMap<Field, Object>();
	private Field searchedKey = null;
	private Object searchFrom = null, searchTo = null;
	private Integer limit = null;

	public ClassConstraintBuilder(Class<T> clazz) {
		this.clazz = clazz;
	}

	Class<? extends PersistingElement> getClazz() {
		return this.clazz;
	}

	Map<Field, Object> getKeyValues() {
		return keyValues;
	}

	Field getSearchedKey() {
		return searchedKey;
	}

	Object getSearchFrom() {
		return searchFrom;
	}

	Object getSearchTo() {
		return searchTo;
	}

	Integer getLimit() {
		return limit;
	}

	void addKeyValue(Field key, Object value) {
		this.keyValues.put(key, value);
	}
	
	void setSearchedKey(Field key, Object startValue, Object endValue) {
		this.searchedKey = key;
		if (startValue != null)
			this.searchFrom = startValue;
		if (endValue != null)
			this.searchTo = endValue;
	}

	void setLimit(int limit) {
		if (this.limit != null) {
			throw new IllegalArgumentException("A limit is already set to " + this.limit);
		}
		this.limit = limit;
	}
	
	public Constraint getConstraint() {
		return this.keyValues.isEmpty() && this.searchedKey == null ? null : new Constraint(this.clazz, this.keyValues, this.searchedKey, this.searchFrom, this.searchTo, true);
	}
	
	/**
	 * Runs the query to find at most N matching elements. The maximum limit N must be set before using {@link ClassConstraintBuilder#withKey(String)}.
	 * Elements are not activated, but their keys are all loaded into memory.
	 * @return A (possibly empty) set of elements matching the query limited to the maximum limit.
	 * @throws DatabaseNotReachedException
	 */
	public Set<T> go() throws DatabaseNotReachedException {
		if (this.limit == null || this.limit < 1)
			throw new IllegalStateException("No limit set ; please use withAtMost expression.");
		return StorageManagement.findElementsToSet(this.clazz, this.getConstraint(), this.limit);
	}
	
	/**
	 * Runs the query to find at most N matching elements. The maximum limit N must be set before using {@link ClassConstraintBuilder#withKey(String)}.
	 * Elements are not activated.
	 * @return A (possibly empty) set of elements matching the query limited to the maximum limit.
	 * @throws DatabaseNotReachedException
	 */
	public CloseableIterator<T> iterate() throws DatabaseNotReachedException {
		if (this.limit == null || this.limit < 1)
			throw new IllegalStateException("No limit set ; please use withAtMost expression.");
		return StorageManagement.findElement(this.clazz, this.getConstraint(), this.limit);
	}

	
	/**
	 * Runs the query to find . Any limit set by {@link ClassConstraintBuilder#withKey(String)} will be ignored.
	 * The element is not activated.
	 * @return A (possibly null) element matching the query.
	 * @throws DatabaseNotReachedException
	 */
	public T any()  throws DatabaseNotReachedException {
		CloseableIterator<T> found = StorageManagement.findElement(this.clazz, this.getConstraint(), 1);
		try {
			if (found.hasNext())
				return found.next();
			else
				return null;
		} finally {
			found.close();
		}
	}

	public KeyConstraintBuilder<T> andWithKey(String key) {
		return this.withKey(key);
	}

	public KeyConstraintBuilder<T> withKey(String key) {
		if (this.clazz == null) {
			throw new IllegalArgumentException("Please state the searched element's class before constraining the keys.");
		}
		for (Field f : KeyManagement.getInstance().detectKeys(this.clazz)) {
			if (f.getName().equals(key)) {
				return new KeyConstraintBuilder<T>(this, f);
			}
		}
		throw new IllegalArgumentException("No key " + key + " found in class " + this.clazz + " ; is this an attribute of the class or one of its superclass annotated with " + Key.class + " ?");
	}
	
	public LimitConstraintBuilder<T> withAtMost(int limit) {
		return new LimitConstraintBuilder<T>(this, limit);
	}
}
