package com.googlecode.n_orm.query;

import java.lang.reflect.Field;
import java.util.NavigableSet;

import com.googlecode.n_orm.CloseableIterator;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.StorageManagement;

public class SearchableClassConstraintBuilder<T extends PersistingElement>
		extends ClassConstraintBuilder<T> {

	private Integer limit = null;
	private String [] toBeActivated = null; //null: no activation, non null: autoactivation


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
	
	@Override
	@SuppressWarnings("unchecked")
	Class<T> getClazz() {
		return (Class<T>) super.getClazz();
	}

	@Override
	public KeyConstraintBuilder<T> createKeyBuilder(
			Field f) {
		return new SearchableKeyConstraintBuilder<T>(this, f);
	}

	public LimitConstraintBuilder<T> withAtMost(int limit) {
		return new LimitConstraintBuilder<T>(this, limit);
	}

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
	 * Runs the query to find at most N matching elements. The maximum limit N must be set before using {@link ClassConstraintBuilder#withKey(String)}.
	 * Elements are not activated, but their keys are all loaded into memory.
	 * @return A (possibly empty) set of elements matching the query limited to the maximum limit.
	 * @throws DatabaseNotReachedException
	 */
	public NavigableSet<T> go() throws DatabaseNotReachedException {
		if (this.limit == null || this.limit < 1)
			throw new IllegalStateException("No limit set ; please use withAtMost expression.");
		return StorageManagement.findElementsToSet(this.getClazz(), this.getConstraint(), this.limit, this.toBeActivated);
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
		return StorageManagement.findElement(this.getClazz(), this.getConstraint(), this.limit, this.toBeActivated);
	}

	
	/**
	 * Runs the query to find . Any limit set by {@link ClassConstraintBuilder#withKey(String)} will be ignored.
	 * The element is not activated.
	 * @return A (possibly null) element matching the query.
	 * @throws DatabaseNotReachedException
	 */
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

	public SearchableKeyConstraintBuilder<T> withKey(String key) {
		return (SearchableKeyConstraintBuilder<T>) this.withKeyInt(key);
	}

	public SearchableKeyConstraintBuilder<T> andWithKey(String key) {
		return this.withKey(key);
	}
}
