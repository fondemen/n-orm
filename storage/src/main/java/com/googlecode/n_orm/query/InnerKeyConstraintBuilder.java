package com.googlecode.n_orm.query;

import java.lang.reflect.Field;

import com.googlecode.n_orm.PersistingElement;

public class InnerKeyConstraintBuilder<T extends PersistingElement> extends KeyConstraintBuilder<T> {

	public InnerKeyConstraintBuilder(
			InnerClassConstraintBuilder<T> cb, Field key) {
		super(cb, key);
	}

	public InnerClassConstraintBuilder<T> setTo(Object value) {
		return (InnerClassConstraintBuilder<T>) super.setToInt(value);
	}

	public InnerClassConstraintBuilder<T> lessOrEqualsThan(Object value) {
		return (InnerClassConstraintBuilder<T>) super.lessOrEqualsThanInt(value);
	}

	public InnerClassConstraintBuilder<T> greaterOrEqualsThan(Object value) {
		return (InnerClassConstraintBuilder<T>) super.greaterOrEqualsThanInt(value);
	}
	
	public InnerRangeKeyConstraintBuilder<T> between(Object value) {
		return new InnerRangeKeyConstraintBuilder<T>((InnerClassConstraintBuilder<T>) this.getConstraintBuilder(), this.getKey(), value);
	}

}
