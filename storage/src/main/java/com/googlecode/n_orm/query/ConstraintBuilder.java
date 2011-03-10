package com.googlecode.n_orm.query;


import com.googlecode.n_orm.PersistingElement;

public class ConstraintBuilder {
	
	public <T extends PersistingElement> ClassConstraintBuilder<T> ofClass(Class<T> clazz) {
		return new ClassConstraintBuilder<T>(clazz);
	}

}
