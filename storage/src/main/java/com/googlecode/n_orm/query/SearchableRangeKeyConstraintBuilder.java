package com.googlecode.n_orm.query;

import java.lang.reflect.Field;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.consoleannotations.Continuator;

public class SearchableRangeKeyConstraintBuilder<T extends PersistingElement> extends
		RangeKeyConstraintBuilder<T> {

	SearchableRangeKeyConstraintBuilder(
			SearchableClassConstraintBuilder<T> cb, Field key,
			Object startValue) {
		super(cb, key, startValue);
	}

	@Continuator
	public SearchableClassConstraintBuilder<T> and(Object includedEndValue) {
		return (SearchableClassConstraintBuilder<T>) super.andInt(includedEndValue);
	}

}
