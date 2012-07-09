package com.googlecode.n_orm.query;

import java.lang.reflect.Field;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.consoleannotations.Continuator;

public class InnerRangeKeyConstraintBuilder<T extends PersistingElement> extends
		RangeKeyConstraintBuilder<T> {

	InnerRangeKeyConstraintBuilder(
			InnerClassConstraintBuilder<T> cb, Field key,
			Object startValue) {
		super(cb, key, startValue);
	}

	@Continuator
	public InnerClassConstraintBuilder<T> and(Object includedEndValue) {
		return (InnerClassConstraintBuilder<T>) super.andInt(includedEndValue);
	}

}
