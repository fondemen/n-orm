package com.googlecode.n_orm.query;


import com.googlecode.n_orm.PersistingElement;

public class ConstraintBuilder {
	
	public <T extends PersistingElement> SearchableClassConstraintBuilder<T> ofClass(Class<T> clazz) {
		return new SearchableClassConstraintBuilder<T>(clazz);
	}

}
