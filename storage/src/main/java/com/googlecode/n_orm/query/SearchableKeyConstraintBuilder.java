package com.googlecode.n_orm.query;

import java.lang.reflect.Field;
import java.util.Date;

import com.googlecode.n_orm.BookStore;
import com.googlecode.n_orm.PersistingElement;


public class SearchableKeyConstraintBuilder<T extends PersistingElement> extends KeyConstraintBuilder<T> {

	public SearchableKeyConstraintBuilder(ClassConstraintBuilder<T> cb,
			Field key) {
		super(cb, key);
	}

	public SearchableClassConstraintBuilder<T> setTo(Object value) {
		return (SearchableClassConstraintBuilder<T>) super.setToInt(value);
	}

	public SearchableClassConstraintBuilder<T> lessOrEqualsThan(Object value) {
		return (SearchableClassConstraintBuilder<T>) super.lessOrEqualsThanInt(value);
	}

	public SearchableClassConstraintBuilder<T> greaterOrEqualsThan(Object value) {
		return (SearchableClassConstraintBuilder<T>) super.greaterOrEqualsThanInt(value);
	}
}
