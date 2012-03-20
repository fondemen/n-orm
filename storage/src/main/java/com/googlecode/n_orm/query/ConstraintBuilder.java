package com.googlecode.n_orm.query;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.consoleannotations.Continuator;

public class ConstraintBuilder {
	
	@Continuator
	public <T extends PersistingElement> SearchableClassConstraintBuilder<T> ofClass(Class<T> clazz) {
		return new SearchableClassConstraintBuilder<T>(clazz);
	}

}
