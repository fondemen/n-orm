package com.googlecode.n_orm.query;

import java.lang.reflect.Field;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.consoleannotations.Continuator;


public class SearchableKeyConstraintBuilder<T extends PersistingElement> extends KeyConstraintBuilder<T> {

	public SearchableKeyConstraintBuilder(ClassConstraintBuilder<T> cb,
			Field key) {
		super(cb, key);
	}
	
	@Continuator
	public SearchableClassConstraintBuilder<T> setTo(Object value) {
		return (SearchableClassConstraintBuilder<T>) super.setToInt(value);
	}

	@Continuator
	public SearchableClassConstraintBuilder<T> lessOrEqualsThan(Object value) {
		return (SearchableClassConstraintBuilder<T>) super.lessOrEqualsThanInt(value);
	}

	@Continuator
	public SearchableClassConstraintBuilder<T> greaterOrEqualsThan(Object value) {
		return (SearchableClassConstraintBuilder<T>) super.greaterOrEqualsThanInt(value);
	}
	
	@Continuator
	public SearchableRangeKeyConstraintBuilder<T> between(Object value) {
		return new SearchableRangeKeyConstraintBuilder<T>((SearchableClassConstraintBuilder<T>) this.getConstraintBuilder(), this.getKey(), value);
	}
}
