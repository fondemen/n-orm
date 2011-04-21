package com.googlecode.n_orm.query;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.HashMap;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.storeapi.Constraint;


/**
 * @param <T> the outermost searched key
 */
public abstract class ClassConstraintBuilder<T extends PersistingElement> {

	private Class<?> clazz;
	private Map<Field, Object> keyValues = new HashMap<Field, Object>();
	private Field searchedKey = null;
	private Object searchFrom = null, searchTo = null;
	private Constraint subConstraint;

	public ClassConstraintBuilder(Class<?> clazz) {
		this.clazz = clazz;
	}

	Class<?> getClazz() {
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

	void addKeyValue(Field key, Object value) {
		this.keyValues.put(key, value);
	}
	
	void setSearchedKey(Field key, Object startValue, Object endValue) {
		if (this.searchedKey != null && !this.searchedKey.equals(key))
			throw new IllegalArgumentException("Searched key is already set to " + searchedKey + " ; cannot perform a search based on bounds for another key (here " + key +")");
		if (this.subConstraint != null)
			throw new IllegalArgumentException("A constraint is alredy given for " + searchedKey);
		if (this.searchedKey != null && !this.searchedKey.equals(key))
			throw new IllegalArgumentException("Searched key (with bouded values) is already set to " + this.searchedKey + " ; cannot search also on " + key + " ; try using setTo on one of those two keys instead.");
		this.searchedKey = key;
		if (startValue != null)
			this.searchFrom = startValue;
		if (endValue != null)
			this.searchTo = endValue;
	}

	Object getSubConstraint() {
		return subConstraint;
	}

	void setSubConstraint(Field key, Constraint subConstraint) {
		if (this.searchedKey != null && !this.searchedKey.equals(key))
			throw new IllegalArgumentException("Searched key is already set to " + this.searchedKey + " ; cannot set it to " + key);
		this.searchedKey = key;
		if (this.subConstraint != null)
			throw new IllegalStateException("A subconstraint is already set for " + this.searchedKey);
		if (this.searchFrom != null || this.searchTo != null)
			throw new IllegalArgumentException("A search range is already set for searched key " + key + " and thus you cannot set a sub constraint using isAnElement");

		this.subConstraint = subConstraint;
	}

	public Constraint getConstraint() {
		if (this.subConstraint == null) {
			if (this.keyValues.isEmpty() && this.searchedKey == null)
				return null;
			else
				return new Constraint(this.clazz, this.keyValues, this.searchedKey, this.searchFrom, this.searchTo, true);
		} else {
			assert searchedKey != null;
			return new Constraint(keyValues, searchedKey, subConstraint);
		}
	}

	protected KeyConstraintBuilder<T> withKeyInt(String key) {
		if (this.clazz == null) {
			throw new IllegalArgumentException("Please state the searched element's class before constraining the keys.");
		}
		for (Field f : KeyManagement.getInstance().detectKeys(this.clazz)) {
			if (f.getName().equals(key)) {
				return this.createKeyBuilder(f);
			}
		}
		throw new IllegalArgumentException("No key " + key + " found in class " + this.clazz + " ; is this an attribute of the class or one of its superclass annotated with " + Key.class + " ?");
	}
	
	public abstract KeyConstraintBuilder<T> createKeyBuilder(Field f);
}
