package com.googlecode.n_orm.query;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.consoleannotations.Continuator;

public class LimitConstraintBuilder<T extends PersistingElement> {
	private final SearchableClassConstraintBuilder<T> constraintBuilder;
	private final int limit;
	
	public LimitConstraintBuilder(SearchableClassConstraintBuilder<T> constraintBuilder,
			int limit) {
		this.constraintBuilder = constraintBuilder;
		this.limit = limit;
	}
	
	@Continuator
	public SearchableClassConstraintBuilder<T> elements() {
		this.constraintBuilder.setLimit(this.limit);
		return this.constraintBuilder;
	}
}
