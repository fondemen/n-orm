package com.googlecode.n_orm.query;

import java.lang.reflect.Field;

import com.googlecode.n_orm.PersistingElement;

public class InnerKeyConstraintBuilder<T extends PersistingElement> extends KeyConstraintBuilder<T> {

	public InnerKeyConstraintBuilder(
			InnerKeyClassConstraintBuilder<T> cb, Field key) {
		super(cb, key);
	}

	public InnerKeyClassConstraintBuilder<T> setTo(String value) {
		return (InnerKeyClassConstraintBuilder<T>) super.setToInt(value);
	}

	public InnerKeyClassConstraintBuilder<T> lessOrEqualsThan(Object value) {
		return (InnerKeyClassConstraintBuilder<T>) super.lessOrEqualsThanInt(value);
	}

	public InnerKeyClassConstraintBuilder<T> greaterOrEqualsThan(Object value) {
		return (InnerKeyClassConstraintBuilder<T>) super.greaterOrEqualsThanInt(value);
	}

}
